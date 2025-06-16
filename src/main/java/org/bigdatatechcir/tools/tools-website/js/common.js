/**
 * 公共JavaScript函数
 */

// 复制文本到剪贴板
function copyToClipboard(text) {
    const textarea = document.createElement('textarea');
    textarea.value = text;
    document.body.appendChild(textarea);
    textarea.select();
    document.execCommand('copy');
    document.body.removeChild(textarea);
}

// 显示通知消息
function showNotification(message, type = 'success') {
    const notification = document.createElement('div');
    notification.className = `notification ${type}`;
    notification.textContent = message;
    
    document.body.appendChild(notification);
    
    // 显示通知
    setTimeout(() => {
        notification.classList.add('show');
    }, 10);
    
    // 3秒后隐藏通知
    setTimeout(() => {
        notification.classList.remove('show');
        setTimeout(() => {
            document.body.removeChild(notification);
        }, 300);
    }, 3000);
}

// 格式化日期
function formatDate(date) {
    const year = date.getFullYear();
    const month = String(date.getMonth() + 1).padStart(2, '0');
    const day = String(date.getDate()).padStart(2, '0');
    const hours = String(date.getHours()).padStart(2, '0');
    const minutes = String(date.getMinutes()).padStart(2, '0');
    const seconds = String(date.getSeconds()).padStart(2, '0');
    
    return `${year}-${month}-${day} ${hours}:${minutes}:${seconds}`;
}

// 添加语法高亮
function addSyntaxHighlighting(element, language) {
    const code = element.textContent;
    let highlighted = code;
    
    switch(language) {
        case 'json':
            highlighted = highlightJSON(code);
            break;
        case 'sql':
            highlighted = highlightSQL(code);
            break;
        case 'js':
            highlighted = highlightJS(code);
            break;
        case 'xml':
            highlighted = highlightXML(code);
            break;
        case 'java':
            highlighted = highlightJava(code);
            break;
    }
    
    element.innerHTML = highlighted;
}

// JSON语法高亮
function highlightJSON(code) {
    return code
        .replace(/"([^"]+)":/g, '<span class="hljs-property">"$1"</span>:')
        .replace(/"([^"]*)"/g, '<span class="hljs-string">"$1"</span>')
        .replace(/\b(true|false)\b/g, '<span class="hljs-boolean">$1</span>')
        .replace(/\b(null)\b/g, '<span class="hljs-null">$1</span>')
        .replace(/\b(\d+)\b/g, '<span class="hljs-number">$1</span>')
        .replace(/([{}[\],:])/g, '<span class="hljs-punctuation">$1</span>');
}

// SQL语法高亮
function highlightSQL(code) {
    const keywords = [
        'SELECT', 'FROM', 'WHERE', 'INSERT', 'UPDATE', 'DELETE', 'CREATE', 'DROP', 'ALTER',
        'TABLE', 'DATABASE', 'VIEW', 'INDEX', 'JOIN', 'LEFT', 'RIGHT', 'INNER', 'OUTER',
        'GROUP', 'ORDER', 'BY', 'HAVING', 'AS', 'ON', 'AND', 'OR', 'NOT', 'NULL', 'IS',
        'IN', 'BETWEEN', 'LIKE', 'ASC', 'DESC', 'DISTINCT', 'CASE', 'WHEN', 'THEN', 'ELSE', 'END'
    ];
    
    let highlighted = code;
    
    // 高亮关键字
    keywords.forEach(keyword => {
        const regex = new RegExp(`\\b${keyword}\\b`, 'gi');
        highlighted = highlighted.replace(regex, match => `<span class="hljs-keyword">${match}</span>`);
    });
    
    // 高亮字符串
    highlighted = highlighted
        .replace(/'([^']*)'/g, '<span class="hljs-string">\'$1\'</span>')
        .replace(/"([^"]*)"/g, '<span class="hljs-string">"$1"</span>')
        .replace(/\b(\d+)\b/g, '<span class="hljs-number">$1</span>');
    
    return highlighted;
}

// JavaScript语法高亮
function highlightJS(code) {
    const keywords = [
        'var', 'let', 'const', 'function', 'return', 'if', 'else', 'for', 'while', 'do',
        'switch', 'case', 'default', 'break', 'continue', 'new', 'this', 'typeof', 'instanceof',
        'true', 'false', 'null', 'undefined', 'class', 'extends', 'super', 'import', 'export',
        'try', 'catch', 'finally', 'throw', 'async', 'await'
    ];
    
    let highlighted = code;
    
    // 高亮关键字
    keywords.forEach(keyword => {
        const regex = new RegExp(`\\b${keyword}\\b`, 'g');
        highlighted = highlighted.replace(regex, match => `<span class="hljs-keyword">${match}</span>`);
    });
    
    // 高亮字符串
    highlighted = highlighted
        .replace(/'([^']*)'/g, '<span class="hljs-string">\'$1\'</span>')
        .replace(/"([^"]*)"/g, '<span class="hljs-string">"$1"</span>')
        .replace(/\b(\d+)\b/g, '<span class="hljs-number">$1</span>');
    
    return highlighted;
}

// XML语法高亮
function highlightXML(code) {
    return code
        .replace(/&lt;!--[\s\S]*?--&gt;/g, '<span class="hljs-comment">$&</span>')
        .replace(/(&lt;\/?)([a-zA-Z][a-zA-Z0-9:]*)([\s\S]*?)(\/?&gt;)/g, 
            '$1<span class="hljs-keyword">$2</span>$3$4')
        .replace(/="([^"]*)"/g, '=<span class="hljs-string">"$1"</span>');
}

// Java语法高亮
function highlightJava(code) {
    const keywords = [
        'abstract', 'assert', 'boolean', 'break', 'byte', 'case', 'catch', 'char', 'class', 'const',
        'continue', 'default', 'do', 'double', 'else', 'enum', 'extends', 'final', 'finally', 'float',
        'for', 'goto', 'if', 'implements', 'import', 'instanceof', 'int', 'interface', 'long', 'native',
        'new', 'package', 'private', 'protected', 'public', 'return', 'short', 'static', 'strictfp', 'super',
        'switch', 'synchronized', 'this', 'throw', 'throws', 'transient', 'try', 'void', 'volatile', 'while',
        'true', 'false', 'null'
    ];
    
    let highlighted = code;
    
    // 高亮关键字
    keywords.forEach(keyword => {
        const regex = new RegExp(`\\b${keyword}\\b`, 'g');
        highlighted = highlighted.replace(regex, match => `<span class="hljs-keyword">${match}</span>`);
    });
    
    // 高亮字符串
    highlighted = highlighted
        .replace(/'([^']*)'/g, '<span class="hljs-string">\'$1\'</span>')
        .replace(/"([^"]*)"/g, '<span class="hljs-string">"$1"</span>')
        .replace(/\b(\d+)\b/g, '<span class="hljs-number">$1</span>');
    
    return highlighted;
}

// 转义HTML特殊字符
function escapeHTML(text) {
    return text
        .replace(/&/g, '&amp;')
        .replace(/</g, '&lt;')
        .replace(/>/g, '&gt;')
        .replace(/"/g, '&quot;')
        .replace(/'/g, '&#039;');
}

// 添加通知样式
document.addEventListener('DOMContentLoaded', function() {
    const style = document.createElement('style');
    style.textContent = `
        .notification {
            position: fixed;
            top: 20px;
            right: 20px;
            padding: 15px 20px;
            border-radius: 4px;
            color: white;
            font-weight: bold;
            opacity: 0;
            transform: translateY(-20px);
            transition: opacity 0.3s, transform 0.3s;
            z-index: 1000;
        }
        
        .notification.show {
            opacity: 1;
            transform: translateY(0);
        }
        
        .notification.success {
            background-color: #4CAF50;
        }
        
        .notification.error {
            background-color: #f44336;
        }
    `;
    document.head.appendChild(style);
}); 