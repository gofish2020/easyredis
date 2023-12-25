package utils

import (
	"fmt"
	"os"
	"path"

	"github.com/kardianos/osext"
)

// ExecDir 当前可执行程序目录
func ExecDir() string {

	path, err := osext.ExecutableFolder()
	if err != nil {
		return ""
	}
	return path
}

func FileExists(filename string) bool {
	info, err := os.Stat(filename)
	return err == nil && !info.IsDir()
}

// 打开文件（如果文件不存在自动创建）
func OpenFile(fileName, dir string) (*os.File, error) {
	// 校验是否有该目录权限
	if checkPermission(dir) {
		return nil, fmt.Errorf("permission denied dir: %s", dir)
	}
	// 创建目录（目录存在啥也不做）
	if err := MakeDir(dir); err != nil {
		return nil, fmt.Errorf("error during make dir %s, err: %s", dir, err)
	}

	// 打开文件（不存在会自动创建），O_APPEND 追加写（权限 读/写）
	f, err := os.OpenFile(path.Join(dir, fileName), os.O_APPEND|os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return nil, fmt.Errorf("fail to open file, err: %s", err)
	}
	return f, nil
}

func checkPermission(src string) bool {
	_, err := os.Stat(src)
	return os.IsPermission(err)
}

func MakeDir(src string) error {
	return os.MkdirAll(src, os.ModePerm)
}
