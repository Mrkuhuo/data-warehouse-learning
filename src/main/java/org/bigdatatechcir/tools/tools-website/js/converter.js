/**
 * 代码转换工具
 */
document.addEventListener('DOMContentLoaded', function() {
    // 获取DOM元素
    const convertInput = document.getElementById('convert-input');
    const convertBtn = document.getElementById('convert-btn');
    const clearBtn = document.getElementById('clear-convert-btn');
    const copyBtn = document.getElementById('copy-convert-btn');
    const convertOutput = document.getElementById('convert-output');
    const convertOptions = document.getElementsByName('convert-type');
    
    // 当前选中的转换类型
    let currentConvertType = 'json-to-java';
    
    // 监听转换类型选择变化
    convertOptions.forEach(option => {
        option.addEventListener('change', function() {
            currentConvertType = this.value;
        });
    });
    
    // 转换按钮点击事件
    convertBtn.addEventListener('click', function() {
        const code = convertInput.value.trim();
        if (!code) {
            showNotification('请输入需要转换的代码', 'error');
            return;
        }
        
        try {
            let convertedCode = '';
            
            switch(currentConvertType) {
                case 'json-to-java':
                    convertedCode = jsonToJava(code);
                    break;
                case 'sql-to-java':
                    convertedCode = sqlToJava(code);
                    break;
            }
            
            convertOutput.textContent = convertedCode;
            addSyntaxHighlighting(convertOutput, 'java');
        } catch (error) {
            convertOutput.textContent = '转换错误: ' + error.message;
            showNotification('转换错误: ' + error.message, 'error');
        }
    });
    
    // 清空按钮点击事件
    clearBtn.addEventListener('click', function() {
        convertInput.value = '';
        convertOutput.textContent = '';
        convertInput.focus();
    });
    
    // 复制按钮点击事件
    copyBtn.addEventListener('click', function() {
        if (convertOutput.textContent) {
            copyToClipboard(convertOutput.textContent);
            showNotification('已复制到剪贴板');
        }
    });
    
    // JSON转Java
    function jsonToJava(jsonStr) {
        try {
            const jsonObj = JSON.parse(jsonStr);
            
            // 生成Java类名
            const className = 'JsonObject';
            
            // 生成Java代码
            let javaCode = `import java.util.List;\nimport java.util.Map;\n\npublic class ${className} {\n`;
            
            // 生成字段和getter/setter
            javaCode += generateJavaFields(jsonObj);
            
            // 结束类定义
            javaCode += '}\n';
            
            return javaCode;
        } catch (error) {
            throw new Error('JSON解析错误: ' + error.message);
        }
    }
    
    // 生成Java字段和getter/setter
    function generateJavaFields(obj, prefix = '') {
        let fields = '';
        
        if (Array.isArray(obj)) {
            // 处理数组
            if (obj.length > 0) {
                const item = obj[0];
                const itemType = getJavaType(item);
                
                if (typeof item === 'object' && item !== null) {
                    // 生成嵌套类
                    const nestedClassName = prefix ? prefix.charAt(0).toUpperCase() + prefix.slice(1) + 'Item' : 'Item';
                    fields += `    private List<${nestedClassName}> ${prefix || 'items'};\n\n`;
                    
                    fields += `    public List<${nestedClassName}> get${capitalizeFirstLetter(prefix || 'items')}() {\n`;
                    fields += `        return ${prefix || 'items'};\n`;
                    fields += `    }\n\n`;
                    
                    fields += `    public void set${capitalizeFirstLetter(prefix || 'items')}(List<${nestedClassName}> ${prefix || 'items'}) {\n`;
                    fields += `        this.${prefix || 'items'} = ${prefix || 'items'};\n`;
                    fields += `    }\n\n`;
                    
                    fields += `    public static class ${nestedClassName} {\n`;
                    fields += generateJavaFields(item, '').split('\n').map(line => '    ' + line).join('\n');
                    fields += '\n    }\n\n';
                } else {
                    fields += `    private List<${itemType}> ${prefix || 'items'};\n\n`;
                    
                    fields += `    public List<${itemType}> get${capitalizeFirstLetter(prefix || 'items')}() {\n`;
                    fields += `        return ${prefix || 'items'};\n`;
                    fields += `    }\n\n`;
                    
                    fields += `    public void set${capitalizeFirstLetter(prefix || 'items')}(List<${itemType}> ${prefix || 'items'}) {\n`;
                    fields += `        this.${prefix || 'items'} = ${prefix || 'items'};\n`;
                    fields += `    }\n\n`;
                }
            }
        } else if (typeof obj === 'object' && obj !== null) {
            // 处理对象
            for (const key in obj) {
                if (obj.hasOwnProperty(key)) {
                    const value = obj[key];
                    const fieldName = key;
                    const fieldType = getJavaType(value);
                    
                    if (typeof value === 'object' && value !== null && !Array.isArray(value)) {
                        // 生成嵌套类
                        const nestedClassName = capitalizeFirstLetter(fieldName);
                        fields += `    private ${nestedClassName} ${fieldName};\n\n`;
                        
                        fields += `    public ${nestedClassName} get${nestedClassName}() {\n`;
                        fields += `        return ${fieldName};\n`;
                        fields += `    }\n\n`;
                        
                        fields += `    public void set${nestedClassName}(${nestedClassName} ${fieldName}) {\n`;
                        fields += `        this.${fieldName} = ${fieldName};\n`;
                        fields += `    }\n\n`;
                        
                        fields += `    public static class ${nestedClassName} {\n`;
                        fields += generateJavaFields(value, '').split('\n').map(line => '    ' + line).join('\n');
                        fields += '\n    }\n\n';
                    } else if (Array.isArray(value)) {
                        // 处理数组字段
                        if (value.length > 0) {
                            const item = value[0];
                            
                            if (typeof item === 'object' && item !== null) {
                                // 生成嵌套类
                                const nestedClassName = capitalizeFirstLetter(fieldName) + 'Item';
                                fields += `    private List<${nestedClassName}> ${fieldName};\n\n`;
                                
                                fields += `    public List<${nestedClassName}> get${capitalizeFirstLetter(fieldName)}() {\n`;
                                fields += `        return ${fieldName};\n`;
                                fields += `    }\n\n`;
                                
                                fields += `    public void set${capitalizeFirstLetter(fieldName)}(List<${nestedClassName}> ${fieldName}) {\n`;
                                fields += `        this.${fieldName} = ${fieldName};\n`;
                                fields += `    }\n\n`;
                                
                                fields += `    public static class ${nestedClassName} {\n`;
                                fields += generateJavaFields(item, '').split('\n').map(line => '    ' + line).join('\n');
                                fields += '\n    }\n\n';
                            } else {
                                const itemType = getJavaType(item);
                                fields += `    private List<${itemType}> ${fieldName};\n\n`;
                                
                                fields += `    public List<${itemType}> get${capitalizeFirstLetter(fieldName)}() {\n`;
                                fields += `        return ${fieldName};\n`;
                                fields += `    }\n\n`;
                                
                                fields += `    public void set${capitalizeFirstLetter(fieldName)}(List<${itemType}> ${fieldName}) {\n`;
                                fields += `        this.${fieldName} = ${fieldName};\n`;
                                fields += `    }\n\n`;
                            }
                        } else {
                            fields += `    private List<Object> ${fieldName};\n\n`;
                            
                            fields += `    public List<Object> get${capitalizeFirstLetter(fieldName)}() {\n`;
                            fields += `        return ${fieldName};\n`;
                            fields += `    }\n\n`;
                            
                            fields += `    public void set${capitalizeFirstLetter(fieldName)}(List<Object> ${fieldName}) {\n`;
                            fields += `        this.${fieldName} = ${fieldName};\n`;
                            fields += `    }\n\n`;
                        }
                    } else {
                        // 处理基本类型字段
                        fields += `    private ${fieldType} ${fieldName};\n\n`;
                        
                        fields += `    public ${fieldType} get${capitalizeFirstLetter(fieldName)}() {\n`;
                        fields += `        return ${fieldName};\n`;
                        fields += `    }\n\n`;
                        
                        fields += `    public void set${capitalizeFirstLetter(fieldName)}(${fieldType} ${fieldName}) {\n`;
                        fields += `        this.${fieldName} = ${fieldName};\n`;
                        fields += `    }\n\n`;
                    }
                }
            }
        }
        
        return fields;
    }
    
    // 获取Java类型
    function getJavaType(value) {
        if (value === null) {
            return 'Object';
        }
        
        switch (typeof value) {
            case 'string':
                return 'String';
            case 'number':
                return Number.isInteger(value) ? 'Integer' : 'Double';
            case 'boolean':
                return 'Boolean';
            case 'object':
                if (Array.isArray(value)) {
                    return 'List<Object>';
                }
                return 'Object';
            default:
                return 'Object';
        }
    }
    
    // 首字母大写
    function capitalizeFirstLetter(str) {
        return str.charAt(0).toUpperCase() + str.slice(1);
    }
    
    // SQL转Java
    function sqlToJava(sqlStr) {
        try {
            // 提取表名
            const tableNameMatch = sqlStr.match(/CREATE\s+TABLE\s+(\w+)/i);
            const tableName = tableNameMatch ? tableNameMatch[1] : 'SqlTable';
            
            // 生成Java类名
            const className = capitalizeFirstLetter(tableName);
            
            // 提取字段
            const fieldsMatch = sqlStr.match(/\(([^)]+)\)/);
            if (!fieldsMatch) {
                throw new Error('无法解析SQL字段');
            }
            
            const fieldsStr = fieldsMatch[1];
            const fieldLines = fieldsStr.split(',').map(line => line.trim());
            
            // 生成Java代码
            let javaCode = `import java.util.Date;\nimport java.math.BigDecimal;\n\npublic class ${className} {\n`;
            
            // 生成字段和getter/setter
            for (const fieldLine of fieldLines) {
                const fieldMatch = fieldLine.match(/(\w+)\s+(\w+)(?:\(.*?\))?/);
                if (fieldMatch) {
                    const fieldName = fieldMatch[1];
                    const fieldType = getSqlJavaType(fieldMatch[2]);
                    
                    javaCode += `    private ${fieldType} ${fieldName};\n\n`;
                    
                    javaCode += `    public ${fieldType} get${capitalizeFirstLetter(fieldName)}() {\n`;
                    javaCode += `        return ${fieldName};\n`;
                    javaCode += `    }\n\n`;
                    
                    javaCode += `    public void set${capitalizeFirstLetter(fieldName)}(${fieldType} ${fieldName}) {\n`;
                    javaCode += `        this.${fieldName} = ${fieldName};\n`;
                    javaCode += `    }\n\n`;
                }
            }
            
            // 结束类定义
            javaCode += '}\n';
            
            return javaCode;
        } catch (error) {
            throw new Error('SQL解析错误: ' + error.message);
        }
    }
    
    // 获取SQL类型对应的Java类型
    function getSqlJavaType(sqlType) {
        sqlType = sqlType.toUpperCase();
        
        switch (sqlType) {
            case 'INT':
            case 'INTEGER':
            case 'SMALLINT':
            case 'TINYINT':
                return 'Integer';
            case 'BIGINT':
                return 'Long';
            case 'FLOAT':
                return 'Float';
            case 'DOUBLE':
            case 'DECIMAL':
            case 'NUMERIC':
                return 'BigDecimal';
            case 'CHAR':
            case 'VARCHAR':
            case 'TEXT':
            case 'LONGTEXT':
                return 'String';
            case 'DATE':
            case 'DATETIME':
            case 'TIMESTAMP':
                return 'Date';
            case 'BOOLEAN':
            case 'BIT':
                return 'Boolean';
            case 'BLOB':
            case 'BINARY':
                return 'byte[]';
            default:
                return 'Object';
        }
    }
}); 