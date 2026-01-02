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
	insideArrayDepth := 0
	inObjectInsideArray := false

	for tok := s.Scan(); tok != scanner.EOF; tok = s.Scan() {
		text := s.TokenText()

		if text == "{" {
			insideObjectDepth++
		} else if text == "}" {
			insideObjectDepth--
		}
		if text == "{" {
			insideObjectDepth++
			if insideArrayDepth > 0 {
				inObjectInsideArray = true
			}
		} else if text == "}" {
			insideObjectDepth--
			if insideObjectDepth == 0 {
				inObjectInsideArray = false
			}
		} else if text == "[" {
			insideArrayDepth++
		} else if text == "]" {
			insideArrayDepth--
		}
		if insideObjectDepth >= 1 && (inObjectInsideArray || insideArrayDepth == 0) && tok == scanner.Int {
			corrected.WriteString(`"`)
			corrected.WriteString(text)
			corrected.WriteString(`"`)
			continue
		}

		corrected.WriteString(text)
	}

	return corrected.String()
}

func _charIsInteger(ch uint8) bool {
	return ch >= '0' && ch <= '9'
}

func _charIsWhitespace(ch uint8) bool {
	return ch == ' ' || ch == '\t' || ch == '\n'
}

func RectifyJsonIntKeysByChar(input string) string {
	var corrected strings.Builder
	insideObjectDepth := 0
	insideArrayDepth := 0
	insideString := false
	escaped := false
	inObjectInsideArray := false

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

		// 在非字符串中，且非转义模式时，去判断:
		// 1. 当前是否在 Object 中（计数器表示嵌套的层级）
		// 2. 当前是否处于 Array 中：
		//   - 对于一般的 Array 中的整数不需要特殊处理
		//   - 而 Object 嵌套在数组中则依然需要处理
		if !insideString && !escaped {
			if ch == '{' {
				insideObjectDepth++
				if insideArrayDepth > 0 {
					inObjectInsideArray = true
				}
			} else if ch == '}' {
				insideObjectDepth--
				if insideObjectDepth == 0 {
					inObjectInsideArray = false
				}
			} else if ch == '[' {
				insideArrayDepth++
			} else if ch == ']' {
				insideArrayDepth--
			}
		}

		// 进入修正环节的条件
		// 1. 在对象中
		// 2. 对象嵌套在数组中，或者，不在数组中
		// 3. 不在字符串中
		// 4. 当前字符在 ASCII 码表的整数范围中
		if insideObjectDepth >= 1 && (inObjectInsideArray || insideArrayDepth == 0) && !insideString && _charIsInteger(ch) {
			startIdx := i
			// 连续的整数
			for i < len(input) && _charIsInteger(input[i]) {
				i++ // 由于预读了连续的整数，需要更新读数索引
			}
			i-- // 退出循环时，当前字符已经不是整数了

			// Lookahead to see if the next non-whitespace character is a colon
			// 向前看（这里只是看，不会实际消费字符），跳过空白字符
			lookahead := i + 1
			for lookahead < len(input) && (_charIsWhitespace(input[lookahead])) {
				lookahead++
			}
			// 下一字符是 `:`，说明这是一个键，那么为其用引号包围
			if lookahead < len(input) && input[lookahead] == ':' {
				corrected.WriteByte('"')
				corrected.WriteString(input[startIdx : i+1])
				corrected.WriteByte('"')
				continue
			} else { // 否则原样输出
				// Write the number without quotes
				corrected.WriteString(input[startIdx : i+1])
				continue
			}
		}
		// 其他字符，无需处理，直接透传
		corrected.WriteByte(ch)
	}

	return corrected.String()
}
