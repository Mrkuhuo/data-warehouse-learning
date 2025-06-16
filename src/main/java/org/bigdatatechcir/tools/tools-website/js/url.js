/**
 * URL编解码工具
 */
document.addEventListener('DOMContentLoaded', function() {
    // 获取DOM元素
    const encodeInput = document.getElementById('url-encode-input');
    const encodeBtn = document.getElementById('url-encode-btn');
    const encodeResult = document.getElementById('url-encode-result');
    const copyEncodeBtn = document.getElementById('copy-encode-btn');
    
    const decodeInput = document.getElementById('url-decode-input');
    const decodeBtn = document.getElementById('url-decode-btn');
    const decodeResult = document.getElementById('url-decode-result');
    const copyDecodeBtn = document.getElementById('copy-decode-btn');
    
    // 编码按钮点击事件
    encodeBtn.addEventListener('click', function() {
        const input = encodeInput.value.trim();
        if (!input) {
            showNotification('请输入需要编码的URL', 'error');
            return;
        }
        
        try {
            const encoded = encodeURIComponent(input);
            encodeResult.value = encoded;
        } catch (error) {
            encodeResult.value = '';
            showNotification('编码错误: ' + error.message, 'error');
        }
    });
    
    // 解码按钮点击事件
    decodeBtn.addEventListener('click', function() {
        const input = decodeInput.value.trim();
        if (!input) {
            showNotification('请输入需要解码的URL', 'error');
            return;
        }
        
        try {
            const decoded = decodeURIComponent(input);
            decodeResult.value = decoded;
        } catch (error) {
            decodeResult.value = '';
            showNotification('解码错误: ' + error.message, 'error');
        }
    });
    
    // 复制编码结果按钮点击事件
    copyEncodeBtn.addEventListener('click', function() {
        if (encodeResult.value) {
            copyToClipboard(encodeResult.value);
            showNotification('已复制到剪贴板');
        }
    });
    
    // 复制解码结果按钮点击事件
    copyDecodeBtn.addEventListener('click', function() {
        if (decodeResult.value) {
            copyToClipboard(decodeResult.value);
            showNotification('已复制到剪贴板');
        }
    });
}); 