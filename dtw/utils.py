import string,random

# 生成随机 6 位字母数字字符串
def random_suffix(length=6):
    chars = string.ascii_lowercase + string.digits
    return ''.join(random.choices(chars, k=length))