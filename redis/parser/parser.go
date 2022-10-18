package parser

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"github.com/hdt3213/godis/interface/redis"
	"github.com/hdt3213/godis/lib/logger"
	"github.com/hdt3213/godis/redis/protocol"
	"io"
	"runtime/debug"
	"strconv"
	"strings"
)

// Payload stores redis.Reply or error
type Payload struct {
	Data redis.Reply
	Err  error
}

// ParseStream reads data from io.Reader and send payloads through channel
func ParseStream(reader io.Reader) <-chan *Payload {
	ch := make(chan *Payload)
	go parse0(reader, ch)
	return ch
}

// ParseBytes reads data from []byte and return all replies
func ParseBytes(data []byte) ([]redis.Reply, error) {
	ch := make(chan *Payload)
	reader := bytes.NewReader(data)
	go parse0(reader, ch)
	var results []redis.Reply
	for payload := range ch {
		if payload == nil {
			return nil, errors.New("no protocol")
		}
		if payload.Err != nil {
			if payload.Err == io.EOF {
				break
			}
			return nil, payload.Err
		}
		results = append(results, payload.Data)
	}
	return results, nil
}

// ParseOne reads data from []byte and return the first payload
func ParseOne(data []byte) (redis.Reply, error) {
	ch := make(chan *Payload)
	reader := bytes.NewReader(data)
	go parse0(reader, ch)
	payload := <-ch // parse0 will close the channel
	if payload == nil {
		return nil, errors.New("no protocol")
	}
	return payload.Data, payload.Err
}

type readState struct {
	readingMultiLine bool
	// 命令参数的字符数
	expectedArgsCount int
	// msgType 的取值为 * $ + - :，分别代表数组 大容量字符串 单行字符串 错误 整型，具体参考Redis的RESP协议
	msgType byte
	args    [][]byte
	// 在读取到 * 或者 $ 时设置，代表着下次到来的数据要读取的字符个数
	// 比如
	bulkLen int64
}

func (s *readState) finished() bool {
	return s.expectedArgsCount > 0 && len(s.args) == s.expectedArgsCount
}

func parse0(reader io.Reader, ch chan<- *Payload) {
	defer func() {
		if err := recover(); err != nil {
			logger.Error(string(debug.Stack()))
		}
	}()
	bufReader := bufio.NewReader(reader)
	var state readState
	var err error
	var msg []byte
	for {
		// read line
		var ioErr bool
		// 根据bufReader读数据，根据bulkLen的值来确认是读一行数据还是读 (bulkLen值+2) 长度的数据
		msg, ioErr, err = readLine(bufReader, &state)
		// 从reader里读取数据失败
		if err != nil {
			// 判断是否是读取失败，是读取失败则关闭管道
			if ioErr { // encounter io err, stop read
				ch <- &Payload{
					Err: err,
				}
				close(ch)
				return
			}
			// protocol err, reset read state
			// 是读取的msg切片最后1个或者1个2个字符不是 \r和\n
			ch <- &Payload{
				Err: err,
			}
			state = readState{}
			continue
		}
		fmt.Printf("从conn连接中获取到的数据 -> %v \n", string(msg))

		// parse line
		// 根据RESP协议，在真正的命令之前，会有 * 号和 $ 号来表示数组长度或者命令参数的字符长度
		// 如果state对象的readingMultiLine字段为false，代表是要读取 * 和 $的
		// 如果state对象的readingMultiLine字段为true，代表要读取用户输入的命令
		if !state.readingMultiLine {
			// receive new response
			// 获取用户输入命令的参数的字符切片数量，也就是参数数量
			// 比如 get name，那么值=2
			if msg[0] == '*' {
				// multi bulk protocol
				err = parseMultiBulkHeader(msg, &state)
				if err != nil {
					ch <- &Payload{
						Err: errors.New("protocol error: " + string(msg)),
					}
					state = readState{} // reset state
					continue
				}
				if state.expectedArgsCount == 0 {
					ch <- &Payload{
						Data: &protocol.EmptyMultiBulkReply{},
					}
					state = readState{} // reset state
					continue
				}
			} else if msg[0] == '$' { // bulk protocol
				err = parseBulkHeader(msg, &state)
				if err != nil {
					ch <- &Payload{
						Err: errors.New("protocol error: " + string(msg)),
					}
					state = readState{} // reset state
					continue
				}
				if state.bulkLen == -1 { // null bulk protocol
					ch <- &Payload{
						Data: &protocol.NullBulkReply{},
					}
					state = readState{} // reset state
					continue
				}
			} else {
				// single line protocol
				result, err := parseSingleLineReply(msg)
				ch <- &Payload{
					Data: result,
					Err:  err,
				}
				state = readState{} // reset state
				continue
			}
		} else {
			// 读取多行
			// receive following bulk protocol
			err = readBody(msg, &state)
			if err != nil {
				ch <- &Payload{
					Err: errors.New("protocol error: " + string(msg)),
				}
				state = readState{} // reset state
				continue
			}
			// if sending finished
			if state.finished() {
				var result redis.Reply
				if state.msgType == '*' {
					result = protocol.MakeMultiBulkReply(state.args)
				} else if state.msgType == '$' {
					result = protocol.MakeBulkReply(state.args[0])
				}
				ch <- &Payload{
					Data: result,
					Err:  err,
				}
				state = readState{}
			}
		}
	}
}

// 根据 state 对象的bulkLen字段来判断
//
// 如果bulkLen==0，则是读一行数据；如果bulkLen != 0，那么则读 (bulkLen字段+2) 长度的数据，然后将bulkLen字段重置为0。
//
// 有三种返回结果：
//
// 1. 如果读取失败则返回 nil,true,err；
//
// 2. 如果是读取消息的长度为0或者msg切片倒数第一个字符或者第一和第二个字符不为 \n和\r时，则返回 nil,false,errors.new("custom")
//
// 3. 读取成功则返回 msg,false,nil
func readLine(bufReader *bufio.Reader, state *readState) ([]byte, bool, error) {
	var msg []byte
	var err error
	if state.bulkLen == 0 { // read normal line
		// 读取RESP协议中的 * 值或者$值，*值代表了用户输入命令的数组长度，比如（get name） 那么*值就为2
		// $值代表了用户输入命令的参数长度，比如（get name），$的取值就是 3或者4，代表着get和name的字符长度
		msg, err = bufReader.ReadBytes('\n')
		if err != nil {
			return nil, true, err
		}
		// 如果读取到的消息长度为0，或者msg切片的倒数第二个位置不是 \r 时则进入if语句
		if len(msg) == 0 || msg[len(msg)-2] != '\r' {
			return nil, false, errors.New("protocol error: " + string(msg))
		}
	} else { // read bulk line (binary safe)
		// 代表已经读取过了*或者$，开始读取命令
		// 比如我输入了 get name 这个命令，走到这里也就开始读取 get 或者 name 了
		msg = make([]byte, state.bulkLen+2)
		_, err = io.ReadFull(bufReader, msg)
		if err != nil {
			return nil, true, err
		}
		if len(msg) == 0 ||
			msg[len(msg)-2] != '\r' ||
			msg[len(msg)-1] != '\n' {
			return nil, false, errors.New("protocol error: " + string(msg))
		}
		// 将下次要读取的参数长度重置为0，下次读取时就要读取 *或者$（参数长度） 了
		state.bulkLen = 0
	}
	return msg, false, nil
}

// 获取用户输入命令的参数个数
// 比如 get name，根据RESP协议，代表用户命令输入个数的*号后面跟的是2,因为有2个参数分别是get 和 name
func parseMultiBulkHeader(msg []byte, state *readState) error {
	var err error
	// 用户输入命令参数的个数
	var expectedLine uint64
	// 将msg切片去头和倒数2位byte，并转成int32赋值给expectedLine
	// 去除 * 号和结尾的 /r和/n，使用切片表达式 [1:2],拿到*号跟的数值
	// 闭区间之所以是2是因为使用了len()函数，msg的长度是4,分别是 *2\r\n，4-2=2
	expectedLine, err = strconv.ParseUint(string(msg[1:len(msg)-2]), 10, 32)
	if err != nil {
		return errors.New("protocol error: " + string(msg))
	}
	if expectedLine == 0 {
		state.expectedArgsCount = 0
		return nil
	} else if expectedLine > 0 {
		// first line of multi bulk protocol
		state.msgType = msg[0]
		state.readingMultiLine = true
		state.expectedArgsCount = int(expectedLine)
		state.args = make([][]byte, 0, expectedLine)
		return nil
	} else {
		return errors.New("protocol error: " + string(msg))
	}
}

func parseBulkHeader(msg []byte, state *readState) error {
	var err error
	// 将切片切割，去头和倒数2位,返回int64类型的数值并赋值给state的bulkLen字段
	state.bulkLen, err = strconv.ParseInt(string(msg[1:len(msg)-2]), 10, 64)
	if err != nil {
		return errors.New("protocol error: " + string(msg))
	}
	if state.bulkLen == -1 { // null bulk
		return nil
	} else if state.bulkLen > 0 {
		state.msgType = msg[0]
		state.readingMultiLine = true
		state.expectedArgsCount = 1
		state.args = make([][]byte, 0, 1)
		return nil
	} else {
		return errors.New("protocol error: " + string(msg))
	}
}

func parseSingleLineReply(msg []byte) (redis.Reply, error) {
	str := strings.TrimSuffix(string(msg), "\r\n")
	var result redis.Reply
	switch msg[0] {
	case '+': // status protocol
		result = protocol.MakeStatusReply(str[1:])
	case '-': // err protocol
		result = protocol.MakeErrReply(str[1:])
	case ':': // int protocol
		val, err := strconv.ParseInt(str[1:], 10, 64)
		if err != nil {
			return nil, errors.New("protocol error: " + string(msg))
		}
		result = protocol.MakeIntReply(val)
	default:
		// parse as text protocol
		strs := strings.Split(str, " ")
		args := make([][]byte, len(strs))
		for i, s := range strs {
			args[i] = []byte(s)
		}
		result = protocol.MakeMultiBulkReply(args)
	}
	return result, nil
}

// read the non-first lines of multi bulk protocol or bulk protocol
func readBody(msg []byte, state *readState) error {
	// 读取命令参数，剔除CRLF字符
	line := msg[0 : len(msg)-2]
	var err error
	if line[0] == '$' {
		// 头一个字符串如果是 $，存储下一次读取的数据长度到state对象的bulkLen字段
		// bulkLen字段会在 readLine 函数中用到，用来读取制定长度的数据
		state.bulkLen, err = strconv.ParseInt(string(line[1:]), 10, 64)
		if err != nil {
			return errors.New("protocol error: " + string(msg))
		}
		if state.bulkLen <= 0 { // null bulk in multi bulks
			state.args = append(state.args, []byte{})
			state.bulkLen = 0
		}
	} else {
		// 获取到命令参数，追加到statue对象的args字段数组中
		state.args = append(state.args, line)
	}
	return nil
}
