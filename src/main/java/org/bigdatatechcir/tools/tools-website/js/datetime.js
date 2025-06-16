/**
 * 日期时间戳转换工具
 */
document.addEventListener('DOMContentLoaded', function() {
    // 获取DOM元素
    const currentTimeBtn = document.getElementById('current-time-btn');
    const currentDatetime = document.getElementById('current-datetime');
    const currentTimestamp = document.getElementById('current-timestamp');
    
    const dateInput = document.getElementById('date-input');
    const dateToTimestampBtn = document.getElementById('date-to-timestamp-btn');
    const dateToTimestampResult = document.getElementById('date-to-timestamp-result');
    
    const timestampInput = document.getElementById('timestamp-input');
    const timestampToDateBtn = document.getElementById('timestamp-to-date-btn');
    const timestampToDateResult = document.getElementById('timestamp-to-date-result');
    
    // 设置日期输入框默认值为当前时间
    const now = new Date();
    dateInput.value = formatDate(now);
    
    // 获取当前时间按钮点击事件
    currentTimeBtn.addEventListener('click', function() {
        const now = new Date();
        currentDatetime.textContent = formatDate(now);
        currentTimestamp.textContent = now.getTime();
    });
    
    // 日期转时间戳按钮点击事件
    dateToTimestampBtn.addEventListener('click', function() {
        const dateStr = dateInput.value.trim();
        if (!dateStr) {
            showNotification('请输入日期', 'error');
            return;
        }
        
        try {
            const date = new Date(dateStr);
            if (isNaN(date.getTime())) {
                throw new Error('无效的日期格式');
            }
            
            dateToTimestampResult.textContent = date.getTime();
        } catch (error) {
            dateToTimestampResult.textContent = '';
            showNotification('日期格式错误: ' + error.message, 'error');
        }
    });
    
    // 时间戳转日期按钮点击事件
    timestampToDateBtn.addEventListener('click', function() {
        const timestampStr = timestampInput.value.trim();
        if (!timestampStr) {
            showNotification('请输入时间戳', 'error');
            return;
        }
        
        try {
            const timestamp = parseInt(timestampStr);
            if (isNaN(timestamp)) {
                throw new Error('无效的时间戳');
            }
            
            const date = new Date(timestamp);
            timestampToDateResult.textContent = formatDate(date);
        } catch (error) {
            timestampToDateResult.textContent = '';
            showNotification('时间戳格式错误: ' + error.message, 'error');
        }
    });
}); 