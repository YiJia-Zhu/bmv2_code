export PCL_BASE_URL='https://llmapi.pcl.ac.cn/v1'
export PCL_API_KEY="sk-Y2WTwzmoMl0nTnjWPjlODiq87HNtdbrZBLEfNDt4ZztXbKDO"

curl --proxy socks5h://127.0.0.1:11451 \
    "$PCL_BASE_URL/chat/completions" \
    -H "Authorization: Bearer $PCL_API_KEY" \
    -H "Content-Type: application/json" \
    -d '{"model":"GLM-5.2","messages":[{"role":"user","content":"你好，请简短介绍自己"}],"max_tokens":1024,"temperature":0,"stream":false}'