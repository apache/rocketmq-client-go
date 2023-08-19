package utils

import (
	"strings"
	"text/scanner"
)

// RectifyJsonIntKeys  token by token fix JSON int type keys issue
func RectifyJsonIntKeys(input string) string {
	var s scanner.Scanner
	s.Init(strings.NewReader(input))

	var corrected strings.Builder
	insideObjectDepth := 0

	for tok := s.Scan(); tok != scanner.EOF; tok = s.Scan() {
		text := s.TokenText()

		if text == "{" {
			insideObjectDepth++
		} else if text == "}" {
			insideObjectDepth--
		}

		if insideObjectDepth >= 1 && tok == scanner.Int {
			corrected.WriteString(`"`)
			corrected.WriteString(text)
			corrected.WriteString(`"`)
			continue
		}

		corrected.WriteString(text)
	}

	return corrected.String()
}

// RectifyJsonIntKeysByChar  character by character fix JSON int type keys issue
func RectifyJsonIntKeysByChar(input string) string {
	var corrected strings.Builder
	insideObjectDepth := 0
	insideString := false
	escaped := false

	for i := 0; i < len(input); i++ {
		ch := input[i]

		if insideString && ch == '\\' { // 在字符串引号范围中，且当前字符是 `\\`
			escaped = true // 开启转义模式
		} else if insideString && ch == '"' && !escaped { // 在字符串中，且当前字符是双引号，且不处于转义状态
			insideString = false // 退出转义模式
		} else if !insideString && ch == '"' { // 不在字符串中，且当前字符是双引号
			insideString = true // 进入字符串模式
		} else { // 否则，消费了一个字符之后
			escaped = false // 关闭转义模式
		}

		// 在非字符串中，且非转义模式时，去判断 Key 是否在 Object 中（计数器表示嵌套的层级）
		if !insideString && !escaped {
			if ch == '{' {
				insideObjectDepth++
			} else if ch == '}' {
				insideObjectDepth--
			}
		}

		// 对象嵌套层级大于1，且不在字符串中，且字符是 0-9 的数字
		if insideObjectDepth >= 1 && !insideString && (ch >= '0' && ch <= '9') {
			// 为该字符加上引号，写入到 Buffer 区
			corrected.WriteByte('"')
			for i < len(input) && input[i] >= '0' && input[i] <= '9' {
				corrected.WriteByte(input[i])
				i++
			}
			i--
			corrected.WriteByte('"')
			continue
		}
		// 其他字符，无需处理，直接透传
		corrected.WriteByte(ch)
	}

	return corrected.String()
}
