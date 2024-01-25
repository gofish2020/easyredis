package parser

import (
	"bufio"
	"bytes"
	"errors"
	"io"
	"runtime/debug"
	"strconv"

	"github.com/gofish2020/easyredis/redis/protocol"
	"github.com/gofish2020/easyredis/tool/logger"
)

type Payload struct {
	Err   error
	Reply protocol.Reply
}

// 从reader读取数据&解析，并保存到chan中，供外部读取
func ParseStream(reader io.Reader) <-chan *Payload {
	dataStream := make(chan *Payload)
	// 启动协程
	go parse(reader, dataStream)
	return dataStream
}

// 从r中读取数据，将读取的结果通过 out chan 发送给外部使用（包括：正常的数据包 or 网络错误）
func parse(r io.Reader, out chan<- *Payload) {

	// 异常恢复，避免未知异常
	defer func() {
		if err := recover(); err != nil {
			logger.Error(err, string(debug.Stack()))
		}
	}()

	reader := bufio.NewReader(r)
	for {

		// 按照 \n 分隔符读取一行数据
		line, err := reader.ReadBytes('\n')
		if err != nil { // 一般是 io.EOF错误（说明conn关闭or文件尾部）
			out <- &Payload{Err: err}
			close(out)
			return
		}
		// 读取到的line中包括 \n 分割符
		length := len(line)

		// RESP协议是按照 \r\n 分割数据
		if length <= 2 || line[length-2] != '\r' { // 说明是空白行，忽略
			continue
		}

		// 去掉尾部 \r\n
		line = bytes.TrimSuffix(line, []byte{'\r', '\n'})

		// 协议文档 ：https://redis.io/docs/reference/protocol-spec/
		// The first byte in an RESP-serialized payload always identifies its type. Subsequent bytes constitute the type's contents.
		switch line[0] {
		case '*': // * 表示数组
			err := parseArrays(line, reader, out)
			if err != nil {
				out <- &Payload{Err: err}
				close(out)
				return
			}
			// + 成功
		case '+':
			out <- &Payload{
				Reply: protocol.NewSimpleReply(string(line[1:])),
			}
			// -  错误
		case '-':
			out <- &Payload{
				Reply: protocol.NewSimpleErrReply(string(line[1:])),
			}

			// $ 二进制安全，字符串
		case '$':
			err = parseBulkString(line, reader, out)
			if err != nil {
				out <- &Payload{Err: err}
				close(out)
				return
			}
		default:
			args := bytes.Split(line, []byte{' '})
			out <- &Payload{
				Reply: protocol.NewMultiBulkReply(args),
			}
		}
	}
}

// 格式： $5\r\nvalue\r\n
func parseBulkString(header []byte, reader *bufio.Reader, out chan<- *Payload) error {

	byteNum, err := strconv.ParseInt(string(header[1:]), 10, 64)
	if err != nil || byteNum < -1 {
		protocolError(out, "illegal bulk string header: "+string(header))
		return nil
	} else if byteNum == -1 { // 空字符串
		out <- &Payload{
			Reply: protocol.NewNullBulkReply(),
		}
		return nil
	}

	body := make([]byte, byteNum+2)
	_, err = io.ReadFull(reader, body)
	if err != nil {
		return err
	}
	out <- &Payload{
		Reply: protocol.NewBulkReply(body[:len(body)-2]),
	}
	return nil
}

/*
数组格式：

*2\r\n
$5\r\n
hello\r\n
$5\r\n
world\r\n

*/

func parseArrays(header []byte, reader *bufio.Reader, out chan<- *Payload) error {
	// 解析 *2 , bodyNum 表示后序有多少个数据等待解析

	bodyNum, err := strconv.ParseInt(string(header[1:]), 10, 64)
	if err != nil || bodyNum < 0 {
		protocolError(out, "illegal array header"+string(header[1:]))
		return nil
	}

	// lines最终保存的解析出来的结果
	lines := make([][]byte, 0, bodyNum)
	// 解析后序数据
	for i := int64(0); i < bodyNum; i++ {
		// 继续读取一行
		var line []byte
		line, err = reader.ReadBytes('\n')
		if err != nil {
			return err
		}

		// 解析 $5\r\n
		length := len(line)
		if length < 4 || line[length-2] != '\r' || line[0] != '$' {
			protocolError(out, "illegal bulk string header "+string(line))
			return nil
		}
		// 得到数字 $5中的数字5
		dataLen, err := strconv.ParseInt(string(line[1:length-2]), 10, 64)
		if err != nil || dataLen < -1 {
			protocolError(out, "illegal bulk string length "+string(line))
			return nil
		} else if dataLen == -1 { // 这里的-1 表示 Null elements in arrays
			lines = append(lines, nil)
		} else {
			// 基于数字5 读取 5+2 长度的数据，这里的2表示\r\n
			body := make([]byte, dataLen+2)
			// 注意：这里直接读取指定长度的字节
			_, err := io.ReadFull(reader, body)
			if err != nil {
				return err
			}
			// 所以最终读取到的是 hello\r\n，去掉\r\n 保存到 lines中
			lines = append(lines, body[:len(body)-2])
		}
	}

	out <- &Payload{
		Err:   nil,
		Reply: protocol.NewMultiBulkReply(lines),
	}
	return nil
}

func protocolError(out chan<- *Payload, msg string) {
	err := errors.New("protocol error: " + msg)
	out <- &Payload{Err: err}
}
