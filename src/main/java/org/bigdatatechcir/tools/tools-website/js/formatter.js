/**
 * 代码格式化及压缩工具
 */
document.addEventListener('DOMContentLoaded', function() {
    // 获取DOM元素
    const inputCode = document.getElementById('input-code');
    const outputCode = document.getElementById('output-code');
    const formatBtn = document.getElementById('format-btn');
    const compressBtn = document.getElementById('compress-btn');
    const clearBtn = document.getElementById('clear-btn');
    const copyBtn = document.getElementById('copy-btn');
    const formatOptions = document.getElementsByName('format');
    
    // 当前选中的格式
    let currentFormat = 'json';
    
    // 监听格式选择变化
    formatOptions.forEach(option => {
        option.addEventListener('change', function() {
            currentFormat = this.value;
            if (inputCode.value.trim()) {
                formatCode();
            }
        });
    });
    
    // 格式化按钮点击事件
    formatBtn.addEventListener('click', formatCode);
    
    // 压缩按钮点击事件
    compressBtn.addEventListener('click', compressCode);
    
    // 清空按钮点击事件
    clearBtn.addEventListener('click', function() {
        inputCode.value = '';
        outputCode.textContent = '';
        inputCode.focus();
    });
    
    // 复制按钮点击事件
    copyBtn.addEventListener('click', function() {
        if (outputCode.textContent) {
            copyToClipboard(outputCode.textContent);
            showNotification('已复制到剪贴板');
        }
    });
    
    // 监听输入框内容变化
    inputCode.addEventListener('input', function() {
        if (inputCode.value.trim()) {
            formatCode();
        }
    });
    
    // 监听输入框粘贴事件
    inputCode.addEventListener('paste', function() {
        setTimeout(function() {
            if (inputCode.value.trim()) {
                formatCode();
            }
        }, 0);
    });
    
    // 监听输入框回车键
    inputCode.addEventListener('keydown', function(e) {
        if (e.key === 'Enter' && !e.shiftKey) {
            formatCode();
        }
    });
    
    // 格式化代码
    function formatCode() {
        const code = inputCode.value.trim();
        if (!code) {
            outputCode.textContent = '';
            return;
        }
        
        try {
            let formattedCode = '';
            
            switch(currentFormat) {
                case 'json':
                    formattedCode = formatJSON(code);
                    break;
                case 'sql':
                    formattedCode = formatSQL(code);
                    break;
                case 'js':
                    formattedCode = formatJS(code);
                    break;
                case 'xml':
                    formattedCode = formatXML(code);
                    break;
            }
            
            outputCode.textContent = formattedCode;
            addSyntaxHighlighting(outputCode, currentFormat);
        } catch (error) {
            outputCode.textContent = '格式化错误: ' + error.message;
            showNotification('格式化错误: ' + error.message, 'error');
        }
    }
    
    // 压缩代码
    function compressCode() {
        const code = inputCode.value.trim();
        if (!code) {
            outputCode.textContent = '';
            return;
        }
        
        try {
            let compressedCode = '';
            
            switch(currentFormat) {
                case 'json':
                    compressedCode = compressJSON(code);
                    break;
                case 'sql':
                    compressedCode = compressSQL(code);
                    break;
                case 'js':
                    compressedCode = compressJS(code);
                    break;
                case 'xml':
                    compressedCode = compressXML(code);
                    break;
            }
            
            outputCode.textContent = compressedCode;
            addSyntaxHighlighting(outputCode, currentFormat);
        } catch (error) {
            outputCode.textContent = '压缩错误: ' + error.message;
            showNotification('压缩错误: ' + error.message, 'error');
        }
    }
    
    // 格式化JSON
    function formatJSON(code) {
        const obj = JSON.parse(code);
        return JSON.stringify(obj, null, 4);
    }
    
    // 压缩JSON
    function compressJSON(code) {
        const obj = JSON.parse(code);
        return JSON.stringify(obj);
    }
    
    // 格式化SQL
    function formatSQL(code) {
        // 简单的SQL格式化
        return code
            .replace(/\s+/g, ' ')
            .replace(/ ?(SELECT|FROM|WHERE|INSERT|UPDATE|DELETE|CREATE|DROP|ALTER|TABLE|DATABASE|JOIN|LEFT|RIGHT|INNER|OUTER|GROUP BY|ORDER BY|HAVING|AND|OR) ?/gi, '\n$1 ')
            .replace(/ ?(,) ?/g, '$1\n    ')
            .trim();
    }
    
    // 压缩SQL
    function compressSQL(code) {
        return code
            .replace(/\s+/g, ' ')
            .trim();
    }
    
    // 格式化JavaScript
    function formatJS(code) {
        // 简单的JS格式化
        let formattedCode = '';
        let indentLevel = 0;
        let inString = false;
        let stringChar = '';
        
        for (let i = 0; i < code.length; i++) {
            const char = code[i];
            const nextChar = code[i + 1] || '';
            
            // 处理字符串
            if ((char === '"' || char === "'" || char === '`') && (i === 0 || code[i - 1] !== '\\')) {
                if (inString && char === stringChar) {
                    inString = false;
                } else if (!inString) {
                    inString = true;
                    stringChar = char;
                }
                formattedCode += char;
                continue;
            }
            
            if (inString) {
                formattedCode += char;
                continue;
            }
            
            // 处理注释
            if (char === '/' && nextChar === '/') {
                // 单行注释
                const commentEnd = code.indexOf('\n', i);
                if (commentEnd === -1) {
                    formattedCode += code.slice(i);
                    break;
                }
                formattedCode += code.slice(i, commentEnd);
                i = commentEnd - 1;
                continue;
            }
            
            if (char === '/' && nextChar === '*') {
                // 多行注释
                const commentEnd = code.indexOf('*/', i + 2);
                if (commentEnd === -1) {
                    formattedCode += code.slice(i);
                    break;
                }
                formattedCode += code.slice(i, commentEnd + 2);
                i = commentEnd + 1;
                continue;
            }
            
            // 处理缩进
            if (char === '{') {
                formattedCode += char;
                indentLevel++;
                formattedCode += '\n' + '    '.repeat(indentLevel);
            } else if (char === '}') {
                indentLevel--;
                formattedCode += '\n' + '    '.repeat(indentLevel) + char;
                if (nextChar !== ';' && nextChar !== ',' && nextChar !== ')') {
                    formattedCode += '\n' + '    '.repeat(indentLevel);
                }
            } else if (char === ';') {
                formattedCode += char;
                if (nextChar !== '}') {
                    formattedCode += '\n' + '    '.repeat(indentLevel);
                }
            } else if (char === '\n') {
                if (formattedCode[formattedCode.length - 1] !== '\n') {
                    formattedCode += '\n' + '    '.repeat(indentLevel);
                }
            } else {
                formattedCode += char;
            }
        }
        
        return formattedCode;
    }
    
    // 压缩JavaScript
    function compressJS(code) {
        return code
            .replace(/\/\/.*$/gm, '') // 移除单行注释
            .replace(/\/\*[\s\S]*?\*\//g, '') // 移除多行注释
            .replace(/\s+/g, ' ') // 将多个空白字符替换为单个空格
            .replace(/\s*([{}:;,=+\-*\/])\s*/g, '$1') // 移除运算符周围的空格
            .replace(/;\}/g, '}') // 移除花括号前的分号
            .trim();
    }
    
    // 格式化XML
    function formatXML(code) {
        let formatted = '';
        let indent = '';
        let tab = '    ';
        
        code.split(/>\s*</).forEach(function(node) {
            if (node.match(/^\/\w/)) {
                indent = indent.substring(tab.length); // 减少缩进
            }
            
            formatted += indent + '<' + node + '>\n';
            
            if (node.match(/^<?\w[^>]*[^\/]$/)) {
                indent += tab; // 增加缩进
            }
        });
        
        return formatted
            .substring(1, formatted.length - 2)
            .replace(/&/g, '&amp;')
            .replace(/</g, '&lt;')
            .replace(/>/g, '&gt;')
            .replace(/"/g, '&quot;')
            .replace(/'/g, '&#039;');
    }
    
    // 压缩XML
    function compressXML(code) {
        return code
            .replace(/>\s+</g, '><') // 移除标签之间的空白
            .replace(/\s+</g, '<') // 移除标签前的空白
            .replace(/>\s+/g, '>') // 移除标签后的空白
            .replace(/\s+/g, ' ') // 将多个空白字符替换为单个空格
            .trim();
    }
}); 