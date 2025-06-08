import inspect
from datetime import datetime
import pprint

def function_to_json(func) -> dict:
    # 定义 Python 类型到 JSON 数据类型的映射
    type_map = {
        str: "string",       # 字符串类型映射为 JSON 的 "string"
        int: "integer",      # 整型类型映射为 JSON 的 "integer"
        float: "number",     # 浮点型映射为 JSON 的 "number"
        bool: "boolean",     # 布尔型映射为 JSON 的 "boolean"
        list: "array",       # 列表类型映射为 JSON 的 "array"
        dict: "object",      # 字典类型映射为 JSON 的 "object"
        type(None): "null",  # None 类型映射为 JSON 的 "null"
    }

    # 获取函数的签名信息
    try:
        signature = inspect.signature(func)
    except ValueError as e:
        # 如果获取签名失败，则抛出异常并显示具体的错误信息
        raise ValueError(
            f"无法获取函数 {func.__name__} 的签名: {str(e)}"
        )

    # 用于存储参数信息的字典
    parameters = {}
    for param in signature.parameters.values():
        # 尝试获取参数的类型，如果无法找到对应的类型则默认设置为 "string"
        try:
            param_type = type_map.get(param.annotation, "string")
        except KeyError as e:
            # 如果参数类型不在 type_map 中，抛出异常并显示具体错误信息
            raise KeyError(
                f"未知的类型注解 {param.annotation}，参数名为 {param.name}: {str(e)}"
            )
        # 将参数名及其类型信息添加到参数字典中
        parameters[param.name] = {"type": param_type}

    # 获取函数中所有必需的参数（即没有默认值的参数）
    required = [
        param.name
        for param in signature.parameters.values()
        if param.default == inspect._empty
    ]

    # 返回包含函数描述信息的字典
    return {
        "type": "function",
        "function": {
            "name": func.__name__,            # 函数的名称
            "description": func.__doc__ or "", # 函数的文档字符串（如果不存在则为空字符串）
            "parameters": {
                "type": "object",
                "properties": parameters,     # 函数参数的类型描述
                "required": required,         # 必须参数的列表
            },
        },
    }

