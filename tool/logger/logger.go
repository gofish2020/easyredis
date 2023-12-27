package logger

import (
	"fmt"
	"log"
	"os"
	"path"
	"runtime"
	"sync"
	"time"

	"github.com/gofish2020/easyredis/utils"
)

/*
purpose: 日志库
*/

const (
	maxLogMessageNum = 1e5
	callerDepth      = 2

	Reset  = "\033[0m"
	Red    = "\033[31m"
	Green  = "\033[32m"
	Blue   = "\033[34m"
	Yellow = "\033[33m"
)

// config for logger example: redis-20231225.log
type Settings struct {
	Path       string `yaml:"path"`        // 路径
	Name       string `yaml:"name"`        // 文件名
	Ext        string `yaml:"ext"`         // 文件后缀
	DateFormat string `yaml:"date-format"` // 日期格式
}

// 日志级别
type LogLevel int

const (
	NULL LogLevel = iota

	FATAL
	ERROR
	WARN
	INFO
	DEBUG
)

var levelFlags = []string{"", "Fatal", "Error", "Warn", "Info", "Debug"}

// 日志消息
type logMessage struct {
	level LogLevel
	msg   string
}

func (m *logMessage) reset() {
	m.level = NULL
	m.msg = ""
}

// 日志底层操作对象
type logger struct {
	logFile    *os.File
	logStd     *log.Logger
	logMsgChan chan *logMessage
	logMsgPool *sync.Pool
	logLevel   LogLevel
	close      chan struct{}
}

func (l *logger) Close() {
	close(l.close)
}
func (l *logger) writeLog(level LogLevel, callerDepth int, msg string) {
	var formattedMsg string
	_, file, line, ok := runtime.Caller(callerDepth)
	if ok {
		formattedMsg = fmt.Sprintf("[%s][%s:%d] %s", levelFlags[level], file, line, msg)
	} else {
		formattedMsg = fmt.Sprintf("[%s] %s", levelFlags[level], msg)
	}

	// 对象池，复用*logMessage对象
	logMsg := l.logMsgPool.Get().(*logMessage)
	logMsg.level = level
	logMsg.msg = formattedMsg
	// 保存到chan缓冲中
	l.logMsgChan <- logMsg
}

var defaultLogger *logger = newStdLogger()

// 构造标准输出日志对象
func newStdLogger() *logger {

	stdLogger := &logger{
		logFile:    nil,
		logStd:     log.New(os.Stdout, "", log.LstdFlags),
		logMsgChan: make(chan *logMessage, maxLogMessageNum),
		logLevel:   DEBUG,
		close:      make(chan struct{}),
		logMsgPool: &sync.Pool{
			New: func() any {
				return &logMessage{}
			},
		},
	}

	go func() {
		// 从缓冲中读取数据
		for {
			select {
			case <-stdLogger.close:
				return
			case logMsg := <-stdLogger.logMsgChan:
				msg := logMsg.msg
				// 根据日志级别，增加不同的颜色
				switch logMsg.level {

				case DEBUG:
					msg = Blue + msg + Reset
				case INFO:
					msg = Green + msg + Reset
				case WARN:
					msg = Yellow + msg + Reset
				case ERROR, FATAL:
					msg = Red + msg + Reset
				}
				stdLogger.logStd.Output(0, msg)
				// 对象池，复用*logMessage对象
				logMsg.reset()
				stdLogger.logMsgPool.Put(logMsg)
			}
		}
	}()

	return stdLogger
}

// 生成输出到文件的日志对象
func newFileLogger(settings *Settings) (*logger, error) {

	fileName := fmt.Sprintf("%s-%s.%s", settings.Name, time.Now().Format(settings.DateFormat), settings.Ext)

	fd, err := utils.OpenFile(fileName, settings.Path)
	if err != nil {
		return nil, fmt.Errorf("newFileLogger.OpenFile err: %s", err)
	}

	fileLogger := &logger{
		logFile:    fd,
		logStd:     log.New(os.Stdout, "", log.LstdFlags),
		logMsgChan: make(chan *logMessage, maxLogMessageNum),
		logLevel:   DEBUG,
		logMsgPool: &sync.Pool{
			New: func() any {
				return &logMessage{}
			},
		},
		close: make(chan struct{}),
	}

	go func() {

		for {
			select {
			case <-fileLogger.close:
				return
			case logMsg := <-fileLogger.logMsgChan:
				//检查是否跨天，重新生成日志文件
				logFilename := fmt.Sprintf("%s-%s.%s", settings.Name, time.Now().Format(settings.DateFormat), settings.Ext)

				if path.Join(settings.Path, logFilename) != fileLogger.logFile.Name() {

					fd, err := utils.OpenFile(logFilename, settings.Path)
					if err != nil {
						panic("open log " + logFilename + " failed: " + err.Error())
					}

					fileLogger.logFile.Close()
					fileLogger.logFile = fd
				}

				msg := logMsg.msg
				// 根据日志级别，增加不同的颜色
				switch logMsg.level {
				case DEBUG:
					msg = Blue + msg + Reset
				case INFO:
					msg = Green + msg + Reset
				case WARN:
					msg = Yellow + msg + Reset
				case ERROR, FATAL:
					msg = Red + msg + Reset
				}
				// 标准输出
				fileLogger.logStd.Output(0, msg)
				// 输出到文件
				fileLogger.logFile.WriteString(time.Now().Format(utils.DateTimeFormat) + " " + logMsg.msg + utils.CRLF)
			}
		}

	}()
	return fileLogger, nil
}

// 程序初始运行的时候调用
func Setup(settings *Settings) {
	defaultLogger.Close()
	logger, err := newFileLogger(settings)
	if err != nil {
		panic(err)
	}
	defaultLogger = logger
}

// 设置日志级别
func SetLoggerLevel(logLevel LogLevel) {
	defaultLogger.logLevel = logLevel
}

// ***********外部调用的日志函数***************
func Debug(v ...any) {
	if defaultLogger.logLevel >= DEBUG {
		msg := fmt.Sprint(v...)
		defaultLogger.writeLog(DEBUG, callerDepth, msg)
	}
}
func Debugf(format string, v ...any) {
	if defaultLogger.logLevel >= DEBUG {
		msg := fmt.Sprintf(format, v...)
		defaultLogger.writeLog(DEBUG, callerDepth, msg)
	}
}

func Info(v ...any) {
	if defaultLogger.logLevel >= INFO {
		msg := fmt.Sprint(v...)
		defaultLogger.writeLog(INFO, callerDepth, msg)
	}
}

func Infof(format string, v ...any) {
	if defaultLogger.logLevel >= INFO {
		msg := fmt.Sprintf(format, v...)
		defaultLogger.writeLog(INFO, callerDepth, msg)
	}
}

func Warn(v ...any) {
	if defaultLogger.logLevel >= WARN {
		msg := fmt.Sprint(v...)
		defaultLogger.writeLog(WARN, callerDepth, msg)
	}
}

func Warnf(format string, v ...any) {
	if defaultLogger.logLevel >= WARN {
		msg := fmt.Sprintf(format, v...)
		defaultLogger.writeLog(WARN, callerDepth, msg)
	}
}

func Error(v ...any) {
	if defaultLogger.logLevel >= ERROR {
		msg := fmt.Sprint(v...)
		defaultLogger.writeLog(ERROR, callerDepth, msg)
	}
}

func Errorf(format string, v ...any) {
	if defaultLogger.logLevel >= ERROR {
		msg := fmt.Sprintf(format, v...)
		defaultLogger.writeLog(ERROR, callerDepth, msg)
	}
}

func Fatal(v ...any) {
	if defaultLogger.logLevel >= FATAL {
		msg := fmt.Sprint(v...)
		defaultLogger.writeLog(FATAL, callerDepth, msg)
	}
}

func Fatalf(format string, v ...any) {
	if defaultLogger.logLevel >= FATAL {
		msg := fmt.Sprintf(format, v...)
		defaultLogger.writeLog(FATAL, callerDepth, msg)
	}
}
