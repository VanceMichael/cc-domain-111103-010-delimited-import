# 合作报表的批次化分隔文本接入

这是一个 Python 数据采集项目。当前代码提供来源材料的基础读取入口，合成输入保存在 fixtures，已有读取行为由 tests 验证。

## 来源材料

weekly.csv 使用 UTF-8 与逗号；semicolon.csv 使用分号，并有跨物理行的引号字段；bom.csv 带 UTF-8 BOM；notes.csv 包含注释和重复表头。当前入口是基础 DictReader，支持显式传入编码和分隔符。

上面的样例用于说明来源字段和当前读取边界。项目按原始来源保存材料；同一来源的版本变化和不同来源的同名记录需要分别核对。示例文件只用于本地调试，不代表真实企业、机构或设备数据。

## 本地运行

项目使用 Python 3.12 或更新版本，仅依赖标准库。从项目根目录执行：

```sh
python3 -m app.inspect
python3 -m unittest discover -s tests -v
python3 -m compileall -q app tests
```

示例只使用本地虚构资料。处理结果放在 runtime 目录，源文件应保持只读。若连接真实来源，先按来源方授权范围配置读取权限。
