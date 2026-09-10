# PSA 访问内网 API 的 SSH/SOCKS 通道

本文记录当前测试环境中，从本机 PSA 访问只有 `my server` 能访问的
`https://llmapi.pcl.ac.cn` 所需的命令。

## 网络拓扑

```text
PSA (本机)
  127.0.0.1:11451
      │ SSH -L
      ▼
vpnserver (10.0.2.2 / 173.16.0.15)
  127.0.0.1:11452
      │ SSH -R
      ▼
Windows
  127.0.0.1:11451  SOCKS5
      │ SSH -D，通过 Windows:2221
      ▼
my server
      │ 内网访问
      ▼
API server: 10.16.17.240:443
  域名: llmapi.pcl.ac.cn
```

`vpnserver` 本身不能直连 `10.16.17.240:443`，因此不能仅靠 DNS 或
静态 `/etc/hosts` 配置解决。域名必须通过 SOCKS5 使用 `socks5h` 传递，
这样目标域名由远端链路解析。

## 域名解析应该配置在哪里

域名解析只需要在 **my server** 上配置，因为 SOCKS5 的最终连接由
my server 发起：

```text
my server 上：llmapi.pcl.ac.cn -> 10.16.17.240
PSA 上：不需要解析 llmapi.pcl.ac.cn
vpnserver 上：不需要解析 llmapi.pcl.ac.cn
Windows 上：不需要解析 llmapi.pcl.ac.cn
```

如果 my server 使用静态 hosts，可在 my server 上确认：

```bash
getent hosts llmapi.pcl.ac.cn
grep -n 'llmapi.pcl.ac.cn' /etc/hosts
```

若尚未配置，可在 my server 上（按实际内网 DNS 或地址调整）添加：

```bash
sudo sh -c 'printf "%s\\n" "10.16.17.240 llmapi.pcl.ac.cn" >> /etc/hosts'
```

PSA 上不要添加下面这类记录：

```text
10.16.17.240 llmapi.pcl.ac.cn
```

PSA 没有到 `10.16.17.240` 的路由，即使解析成功也无法直连。PSA 的请求
必须使用：

```bash
--proxy socks5h://127.0.0.1:11451
```

其中 `socks5h` 的 `h` 表示把主机名交给远端 SOCKS 链路解析。使用普通的
`socks5://` 时，curl 会尝试在 PSA 本地解析域名，可能导致失败。

## 0. API server 和 my server

### API server（`10.16.17.240`）

API server 是被访问的内网目标，不需要在它上面建立隧道或执行命令。只要
它继续监听 `10.16.17.240:443`，并为 `llmapi.pcl.ac.cn` 提供 HTTPS 服务即可。

### my server

my server 是唯一能访问 API server 的跳板。通常不需要在 my server 上执行
额外命令；Windows 的第一条 `ssh -D` 命令会把 SOCKS 连接的实际出口放在
my server 上。需要排查时，可从 my server 直接验证：

```bash
getent hosts llmapi.pcl.ac.cn
curl -vkI --connect-timeout 5 https://llmapi.pcl.ac.cn
```

返回 HTTP 响应（即使是 `401`）即可证明 my server 到 API server 的路径正常。

## 1. Windows：建立到 my server 的 SOCKS5

在 Windows PowerShell 窗口一执行，并保持窗口运行：

```powershell
ssh -NT -p 2221 `
  -D 127.0.0.1:11451 `
  -o ExitOnForwardFailure=yes `
  -o ServerAliveInterval=30 `
  -o ServerAliveCountMax=3 `
  xiegr@127.0.0.1
```

含义：Windows 的 `127.0.0.1:11451` 是 SOCKS5 代理，所有通过该代理的
连接都会从 `my server` 发起。

检查 Windows 本地端口：

```powershell
netstat -ano | findstr 11451
```

## 2. Windows：把 SOCKS5 反向转发到 vpnserver

在 Windows PowerShell 窗口二执行，并保持窗口运行：

```powershell
ssh -NT `
  -R 127.0.0.1:11452:127.0.0.1:11451 `
  -o ExitOnForwardFailure=yes `
  -o ServerAliveInterval=30 `
  -o ServerAliveCountMax=3 `
  vpnserver@173.16.0.15
```

按提示输入 `vpnserver` 的 SSH 密码。该命令的目标是：

```text
vpnserver:127.0.0.1:11452 -> Windows:127.0.0.1:11451 (SOCKS5)
```

如果出现 `administratively prohibited` 或 `remote port forwarding failed`，
在 vpnserver 上检查 SSH 服务配置（需要 sudo 权限）：

```bash
sudo sshd -T | grep -Ei 'allowtcpforwarding|permitopen|permitlisten|gatewayports'
```

至少应看到：

```text
allowtcpforwarding yes
permitopen any
```

## 3. vpnserver：检查反向端口

从 PSA 登录 vpnserver（当前环境已验证 `vpnserver@10.0.2.2` 可用；如果
你的环境配置的是 `p4` 用户，则替换用户名）：

```bash
ssh vpnserver@10.0.2.2
```

在 vpnserver 上执行：

```bash
ss -lntp | grep 11452
```

正常结果应包含：

```text
127.0.0.1:11452
```

## 4. PSA：建立本地 SOCKS 入口

在 PSA 上执行下面命令。`-fNT` 会将隧道放到后台运行：

```bash
ssh -fNT \
  -L 127.0.0.1:11451:127.0.0.1:11452 \
  -o ExitOnForwardFailure=yes \
  -o ServerAliveInterval=30 \
  -o ServerAliveCountMax=3 \
  vpnserver@10.0.2.2
```

含义：

```text
PSA:127.0.0.1:11451 -> vpnserver:127.0.0.1:11452
```

检查 PSA 本地端口：

```bash
ss -lntp | grep 11451
```

如果需要前台运行以便查看错误，去掉 `-f`：

```bash
ssh -NT \
  -L 127.0.0.1:11451:127.0.0.1:11452 \
  -o ExitOnForwardFailure=yes \
  -o ServerAliveInterval=30 \
  vpnserver@10.0.2.2
```

## 5. PSA：测试 API 网络链路

不带 API Key 测试时，收到 HTTP `401 Invalid token` 也表示网络和 TLS
链路已经成功：

```bash
curl -v \
  --connect-timeout 8 \
  --max-time 20 \
  --proxy socks5h://127.0.0.1:11451 \
  https://llmapi.pcl.ac.cn/v1/models
```

注意必须写成 `socks5h://`，不要写成 `socks5://`；`h` 表示由远端 SOCKS
链路解析 `llmapi.pcl.ac.cn`。

## 6. PSA：调用聊天 API

```bash
export PCL_BASE_URL='https://llmapi.pcl.ac.cn/v1'
export PCL_API_KEY='在此填写 API Key'

curl --proxy socks5h://127.0.0.1:11451 \
  "$PCL_BASE_URL/chat/completions" \
  -H "Authorization: Bearer $PCL_API_KEY" \
  -H "Content-Type: application/json" \
  -d '{"model":"GLM-5.2","messages":[{"role":"user","content":"你好，请简短介绍自己"}],"max_tokens":1024,"temperature":0,"stream":false}'
```

当前 `bmv2_code/test.sh` 已包含同类请求；运行它之前，确认本地
`127.0.0.1:11451` 隧道仍在监听。

## 7. 使用 PCL 模型测试 BMv2 编译通过率

`bmv2_llm_compile_benchmark.py` 支持四条独立调用路径：

- `--provider local`：原有部署在 my server 上的本地模型，默认直连
  `http://127.0.0.1:18001/v1`，不经过 SOCKS5。
- `--provider pcl`：从 `.env` 读取 `PCL_BASE_URL` 和 `PCL_API_KEY`，默认经过
  `socks5h://127.0.0.1:11451`。
- `--provider deepseek`：从 `.env` 读取 `DEEPSEEK_BASE`/`DEEPSEEK_KEY`，兼容
  `DEEPSEEK_BASE_URL`/`DEEPSEEK_API_KEY`，默认模型为 `deepseek-chat`，默认直连
  `https://api.deepseek.com/v1`，不经过 PCL 隧道。
- `--provider openai`：从 `.env` 读取 `OPENAI_BASE_URL`/`OPENAI_API_KEY`，默认
  使用 `https://api.vveai.com/v1` 和 `gpt-5.5`；可用 `OPENAI_MODEL` 覆盖模型。

测试原有本地模型：

```bash
cd /home/p4/bmv2_code
./bmv2_llm_compile_benchmark.py \
  --provider local \
  --attempts 10
```

测试单个 PCL 聊天模型：

```bash
./bmv2_llm_compile_benchmark.py \
  --provider pcl \
  --model GLM-5.2 \
  --attempts 10
```

测试 DeepSeek：

```bash
./bmv2_llm_compile_benchmark.py \
  --provider deepseek \
  --model deepseek-chat \
  --prompts data/tasks/bmv2_pilot_prompts.jsonl \
  --attempts 3
```

测试 OpenAI 兼容接口：

```bash
./bmv2_llm_compile_benchmark.py \
  --provider openai \
  --model gpt-5.5 \
  --prompts data/tasks/bmv2_pilot_prompts.jsonl \
  --attempts 3
```

依次测试当前五个 PCL 聊天模型：

```bash
for model in \
  DeepSeek-V4-Flash-0731 \
  DeepSeek-V4-Pro \
  GLM-5.2 \
  PCNL本地大模型 \
  Qwen3.6-35B
do
  ./bmv2_llm_compile_benchmark.py \
    --provider pcl \
    --model "$model" \
    --attempts 10
done
```

每次运行的请求、原始响应、生成的 P4、编译器输出和通过率会保存在
`benchmark_runs/`。`bge-m3`、`bge-reranker` 和 `whisper-1` 不是代码生成
模型，因此脚本会拒绝把它们用于该基准。

如果 SOCKS 地址改变，可在 `.env` 中设置 `PCL_PROXY`，或者传入
`--proxy socks5h://host:port`。只有在运行机器本身可以直接访问 PCL 时才使用
`--direct`。

## 启动顺序

每次 Windows 重启或隧道断开后，按以下顺序重新建立：

1. Windows 窗口一：`-D 127.0.0.1:11451`。
2. Windows 窗口二：`-R 127.0.0.1:11452:127.0.0.1:11451`。
3. PSA：`-L 127.0.0.1:11451:127.0.0.1:11452`。
4. PSA 使用 `curl --proxy socks5h://127.0.0.1:11451 ...`。

## 故障定位

| 现象 | 检查位置 | 说明 |
|---|---|---|
| PSA 没有 `11451` | PSA | 重新执行第 4 节的 `ssh -fNT -L ...` |
| vpnserver 没有 `11452` | Windows 窗口二、vpnserver | 重新执行 `-R ...`；检查 `AllowTcpForwarding` |
| `Unable to receive initial SOCKS5 response` | Windows 窗口一/二 | SOCKS 或反向转发未建立 |
| `Could not resolve host` | PSA curl | 使用 `socks5h://`，不要使用 `socks5://` |
| HTTP `401 Invalid token` | API 请求 | 网络已通，但 API Key 缺失或无效 |
| TLS 证书错误 | 目标域名/代理参数 | 保持 URL 为 `https://llmapi.pcl.ac.cn`，不要改成 IP |

不要在 PSA 上把 `llmapi.pcl.ac.cn` 解析成 `10.16.17.240` 后直接访问；
PSA 没有到该内网地址的可用路由。API Key 不要提交到 Git；此前暴露过的测试
Key 建议测试完成后重新生成。
