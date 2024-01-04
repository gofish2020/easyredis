package wildcard

import (
	"errors"
	"regexp"
	"strings"
)

/*

将redis支持的匹配模式，转换成go支持的正则表达式


Supported glob-style patterns:

h?llo matches hello, hallo and hxllo
h*llo matches hllo and heeeello
h[ae]llo matches hello and hallo, but not hillo
h[^e]llo matches hallo, hbllo, ... but not hello
h[a-b]llo matches hallo and hbllo

除了 ? * [] [^] [-] 这些字符，都作为正常的字符进行匹配(也就是要转义)

*/

type Pattern struct {
	exp *regexp.Regexp
}

var replaceMap = map[byte]string{
	// characters in the wildcard that must be escaped in the regexp
	'+': `\+`, // 这些字符在正则表达式中是有意的，为了作为普通的字符匹配，需要 \ 转义为普通字符
	')': `\)`,
	'$': `\$`,
	'.': `\.`,
	'{': `\{`,
	'}': `\}`,
	'|': `\|`,
	'*': ".*", // * 对应go的 .*
	'?': ".",  // ? 对应go的 .
}

var errEndWithEscape = "end with escape \\"

func CompilePattern(src string) (*Pattern, error) {
	regSrc := strings.Builder{}

	regSrc.WriteByte('^') // 正则表达式-开头

	for i := 0; i < len(src); i++ {
		ch := src[i]
		if ch == '\\' {
			if i == len(src)-1 { // 例如: a\
				return nil, errors.New(errEndWithEscape)
			}
			// 例如 \b
			regSrc.WriteByte('\\')
			regSrc.WriteByte(src[i+1])
			i++ // 将\b一次性全保存
		} else if ch == '^' { // redis中 [^ 是配套的一对
			if i == 0 { // 说明前面没有 [
				regSrc.WriteString(`\^`) // 那^就转义为普通字符 \^
			} else if i == 1 {
				if src[i-1] == '[' {
					regSrc.WriteString(`^`) // src is: [^
				} else {
					regSrc.WriteString(`\^`) // example: a^
				}
			} else {
				if src[i-1] == '[' && src[i-2] != '\\' {
					regSrc.WriteString(`^`) // example:  a[^
				} else {
					regSrc.WriteString(`\^`) // example: \[^
				}
			}
		} else if escaped, toEscape := replaceMap[ch]; toEscape {
			regSrc.WriteString(escaped)
		} else {
			regSrc.WriteByte(ch)
		}
	}
	regSrc.WriteByte('$') // 正则表达式-结尾

	re, err := regexp.Compile(regSrc.String())
	if err != nil {
		return nil, err
	}

	return &Pattern{
		exp: re,
	}, nil
}

func (p *Pattern) IsMatch(s string) bool {
	return p.exp.MatchString(s)
}
