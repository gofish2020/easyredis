package conf

import (
	"bufio"
	"io"
	"os"
	"reflect"
	"strconv"
	"strings"

	"github.com/gofish2020/easyredis/utils"
)

const runidMaxLen = 40
const defaultDatabasesNum = 16

/*
purpose:读取conf配置文件
*/

type RedisConfig struct {
	// 基础配置
	Bind  string `conf:"bind"`
	Port  int    `conf:"port"`
	Dir   string `conf:"dir"`
	RunID string `conf:"runid"`

	// 数据库个数
	Databases int `conf:"databases"`

	// aof 相关
	AppendOnly     bool   `conf:"appendonly"`     // 是否启用aof
	AppendFilename string `conf:"appendfilename"` // aof文件名
	AppendFsync    string `conf:"appendfsync"`    // aof刷盘间隔

	// 服务器密码
	RequirePass string `conf:"requirepass,omitempty"`

	// 集群
	Peers []string `conf:"peers"`
	Self  string   `conf:"self"`
}

// 全局配置
var GlobalConfig *RedisConfig

func init() {
	GlobalConfig = &RedisConfig{
		Bind:       "127.0.0.1",
		Port:       6379,
		AppendOnly: false,
		Dir:        ".",
		RunID:      utils.RandString(runidMaxLen),
		Databases:  defaultDatabasesNum,
	}
}

// 加载配置文件，更新 GlobalConfig 对象
func LoadConfig(configFile string) error {

	//1.打开文件
	file, err := os.Open(configFile)
	if err != nil {
		panic(err)
	}
	defer file.Close()
	//2.解析文件
	GlobalConfig = parse(file)

	//3.补充信息
	GlobalConfig.RunID = utils.RandString(runidMaxLen)
	if GlobalConfig.Dir == "" {
		GlobalConfig.Dir = utils.ExecDir()
	}

	if GlobalConfig.Databases == 0 {
		GlobalConfig.Databases = defaultDatabasesNum
	}

	utils.MakeDir(GlobalConfig.Dir)
	return nil
}

func parse(r io.Reader) *RedisConfig {

	newRedisConfig := &RedisConfig{}

	//1.按行扫描文件
	lineMap := make(map[string]string)
	scanner := bufio.NewScanner(r)

	for scanner.Scan() {
		line := scanner.Text()
		line = strings.TrimLeft(line, " ")

		// 空行 or 注释行
		if len(line) == 0 || (len(line) > 0 && line[0] == '#') {
			continue
		}

		// 解析行  例如: Bind 127.0.0.1
		idx := strings.IndexAny(line, " ")
		if idx > 0 && idx < len(line)-1 {
			key := line[:idx]
			value := strings.Trim(line[idx+1:], " ")
			// 将每行的结果，保存到lineMap中
			lineMap[strings.ToLower(key)] = value
		}
	}

	if err := scanner.Err(); err != nil {
		panic(err.Error())
	}

	//2.将扫描结果保存到newRedisConfig 对象中

	configValue := reflect.ValueOf(newRedisConfig).Elem()
	configType := reflect.TypeOf(newRedisConfig).Elem()

	// 遍历结构体字段（类型）
	for i := 0; i < configType.NumField(); i++ {

		fieldType := configType.Field(i)
		// 读取字段名
		fieldName := strings.Trim(fieldType.Tag.Get("conf"), " ")
		if fieldName == "" {
			fieldName = fieldType.Name
		} else {
			fieldName = strings.Split(fieldName, ",")[0]
		}
		fieldName = strings.ToLower(fieldName)
		// 判断该字段是否在config中有配置
		fieldValue, ok := lineMap[fieldName]

		if ok {
			// 将结果保存到字段中
			switch fieldType.Type.Kind() {
			case reflect.String:
				configValue.Field(i).SetString(fieldValue)
			case reflect.Bool:
				configValue.Field(i).SetBool("yes" == fieldValue)
			case reflect.Int:
				intValue, err := strconv.ParseInt(fieldValue, 10, 64)
				if err == nil {
					configValue.Field(i).SetInt(intValue)
				}
			case reflect.Slice:
				// 切片的元素是字符串
				if fieldType.Type.Elem().Kind() == reflect.String {
					tmpSlice := strings.Split(fieldValue, ",")
					configValue.Field(i).Set(reflect.ValueOf(tmpSlice))
				}
			}
		}
	}
	return newRedisConfig
}

func TmpDir() string {
	dir := GlobalConfig.Dir + "/tmp"
	utils.MakeDir(dir)
	return dir
}
