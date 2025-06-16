/**
 * 代码对比工具
 */
document.addEventListener('DOMContentLoaded', function() {
    // 获取DOM元素
    const originalCode = document.getElementById('original-code');
    const newCode = document.getElementById('new-code');
    const compareBtn = document.getElementById('compare-btn');
    const clearBtn = document.getElementById('clear-diff-btn');
    const diffOriginal = document.getElementById('diff-original');
    const diffNew = document.getElementById('diff-new');
    
    // 对比按钮点击事件
    compareBtn.addEventListener('click', function() {
        const original = originalCode.value;
        const newText = newCode.value;
        
        if (!original.trim() || !newText.trim()) {
            showNotification('请输入需要对比的代码', 'error');
            return;
        }
        
        const diff = computeDiff(original, newText);
        displayDiff(diff);
    });
    
    // 清空按钮点击事件
    clearBtn.addEventListener('click', function() {
        originalCode.value = '';
        newCode.value = '';
        diffOriginal.innerHTML = '';
        diffNew.innerHTML = '';
        originalCode.focus();
    });
    
    // 计算差异
    function computeDiff(original, newText) {
        const originalLines = original.split('\n');
        const newLines = newText.split('\n');
        
        const result = {
            original: [],
            new: []
        };
        
        // 使用最长公共子序列算法计算差异
        const lcs = longestCommonSubsequence(originalLines, newLines);
        
        let originalIndex = 0;
        let newIndex = 0;
        
        for (const item of lcs) {
            // 添加原始文本中的差异行
            while (originalIndex < item.originalIndex) {
                result.original.push({
                    line: originalLines[originalIndex],
                    type: 'remove'
                });
                originalIndex++;
            }
            
            // 添加新文本中的差异行
            while (newIndex < item.newIndex) {
                result.new.push({
                    line: newLines[newIndex],
                    type: 'add'
                });
                newIndex++;
            }
            
            // 添加相同的行
            result.original.push({
                line: originalLines[originalIndex],
                type: 'same'
            });
            
            result.new.push({
                line: newLines[newIndex],
                type: 'same'
            });
            
            originalIndex++;
            newIndex++;
        }
        
        // 添加剩余的差异行
        while (originalIndex < originalLines.length) {
            result.original.push({
                line: originalLines[originalIndex],
                type: 'remove'
            });
            originalIndex++;
        }
        
        while (newIndex < newLines.length) {
            result.new.push({
                line: newLines[newIndex],
                type: 'add'
            });
            newIndex++;
        }
        
        return result;
    }
    
    // 最长公共子序列算法
    function longestCommonSubsequence(originalLines, newLines) {
        const m = originalLines.length;
        const n = newLines.length;
        
        // 创建DP表
        const dp = Array(m + 1).fill().map(() => Array(n + 1).fill(0));
        
        // 填充DP表
        for (let i = 1; i <= m; i++) {
            for (let j = 1; j <= n; j++) {
                if (originalLines[i - 1] === newLines[j - 1]) {
                    dp[i][j] = dp[i - 1][j - 1] + 1;
                } else {
                    dp[i][j] = Math.max(dp[i - 1][j], dp[i][j - 1]);
                }
            }
        }
        
        // 回溯找出LCS
        const lcs = [];
        let i = m, j = n;
        
        while (i > 0 && j > 0) {
            if (originalLines[i - 1] === newLines[j - 1]) {
                lcs.unshift({
                    originalIndex: i - 1,
                    newIndex: j - 1,
                    line: originalLines[i - 1]
                });
                i--;
                j--;
            } else if (dp[i - 1][j] > dp[i][j - 1]) {
                i--;
            } else {
                j--;
            }
        }
        
        return lcs;
    }
    
    // 显示差异
    function displayDiff(diff) {
        diffOriginal.innerHTML = '';
        diffNew.innerHTML = '';
        
        diff.original.forEach(item => {
            const lineElement = document.createElement('div');
            lineElement.className = `diff-line ${item.type === 'remove' ? 'diff-remove' : ''}`;
            lineElement.textContent = item.line;
            diffOriginal.appendChild(lineElement);
        });
        
        diff.new.forEach(item => {
            const lineElement = document.createElement('div');
            lineElement.className = `diff-line ${item.type === 'add' ? 'diff-add' : ''}`;
            lineElement.textContent = item.line;
            diffNew.appendChild(lineElement);
        });
    }
}); 