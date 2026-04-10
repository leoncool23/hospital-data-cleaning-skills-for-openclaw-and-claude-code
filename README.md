# 🏥 hospital-data-cleaning-zh（医院数据清洗技能包·中文版）

**面向医院及公共卫生数据的完整、生产级数据清洗技能包。**

兼容以下平台：
- ✅ **Claude Code**（通过 `CLAUDE.md` 或 skills 目录）
- ✅ **Codex / OpenAI Codex 智能体**（AgentSkills 兼容的 SKILL.md 格式）
- ✅ **OpenClaw**（放入 `~/.openclaw/skills/` 或 `<workspace>/skills/`）

---

## 目录结构

```
hospital-data-cleaning-zh/
├── SKILL.md                              ← 核心入口（必须首先阅读）
├── README.md                             ← 安装与使用说明
├── references/
│   ├── cleaning-rules.md                 ← 临床边界值与逻辑规则
│   ├── encoding-standards.md             ← ICD-10、单位换算、日期格式、缩写词典
│   └── quality-metrics.md               ← 六维质量评分标准 + 质控报告模板
└── scripts/
    ├── hospital_cleaner.py               ← 完整清洗流水线（CLI 可直接运行）
    └── charlson_cci.py                   ← Charlson 合并症指数计算器（ICD-10版）
```

---

## 快速安装

### Claude Code / Codex

将本目录放入项目的 `.claude/skills/` 目录，并在 `CLAUDE.md` 中引用：

```markdown
## 技能
阅读并应用：.claude/skills/hospital-data-cleaning-zh/SKILL.md
```

### OpenClaw — 全局安装（所有 Agent 可用）

```bash
cp -r hospital-data-cleaning-zh ~/.openclaw/skills/
openclaw gateway restart
```

### OpenClaw — 工作区安装（指定 Agent 专用）

```bash
cp -r hospital-data-cleaning-zh ~/.openclaw/workspace-<agentId>/skills/
openclaw gateway restart
```

### OpenClaw 多 Agent 分工

见 `SKILL.md` 末尾的推荐6个专职 Agent 分工方案，可使用 `sessions_spawn` 并行执行计算密集型阶段。

---

## 运行清洗流水线

### 安装依赖

```bash
pip install pandas numpy python-dateutil scikit-learn pyarrow recordlinkage
```

### 基础运行

```bash
python scripts/hospital_cleaner.py --input 原始数据.csv --output output/
```

### 带配置文件运行

```bash
python scripts/hospital_cleaner.py \
  --input 原始数据.parquet \
  --output output/ \
  --config config.json
```

### config.json 示例

```json
{
  "critical_fields": ["患者ID", "入院日期", "ICD10编码"],
  "dedup_keys":      ["患者ID", "入院日期", "检验编码"],
  "deidentify":      true,
  "keep_pseudonym":  false
}
```

---

## 15大清洗类别总览

| 序号 | 类别 | 核心操作 |
|------|------|---------|
| 1 | 缺失值处理 | MCAR/MAR/MNAR分类、MICE插补、缺失指示变量 |
| 2 | 异常值处理 | 硬边界过滤、IQR(k=3)检测、四种子类标记 |
| 3 | 小样本抑制 | n<5规则、二次抑制、差分隐私 |
| 4 | 重复数据处理 | 完全重复行、事件级、患者级(MPI) |
| 5 | 逻辑一致性校验 | 时间逻辑、临床逻辑、流程逻辑 |
| 6 | 编码标准化 | ICD-10、药品、性别、日期 |
| 7 | 时间数据处理 | ISO 8601、住院天数、流行病学周 |
| 8 | 隐私脱敏 | PHI删除、k-匿名、年龄分组 |
| 9 | 数据结构清洗 | 类型规范、宽长表转换、关系规范化 |
| 10 | 文本与NLP清洗 | 缩写扩展、NER命名实体识别 |
| 11 | 数据来源核对 | 跨系统对账、抽样审计、数据血缘 |
| 12 | 衍生变量构建 | CCI合并症指数、BMI、30天再入院 |
| 13 | 数据标准化/缩放 | Z-score、MinMax、独热编码（仅建模前） |
| 14 | 数据集成 | 多系统合并、主键对齐、冲突解决 |
| 15 | 质控与可追溯 | 六维质量评分、质控报告、版本控制 |

---

## 输出文件说明

每次运行后，`output/` 目录下自动生成：

```
output/
├── 00_画像报告_<时间戳>.json       ← 清洗前数据画像
├── 01_清洗日志_<时间戳>.csv        ← 所有变更记录
├── 02_标记记录_<时间戳>.csv        ← 问题记录（待人工审核）
├── 03_清洗数据_<时间戳>.parquet    ← 清洗后数据集
└── 04_质量报告_<时间戳>.md         ← 人类可读质量报告
```

---

## 重要提醒

1. **患者隐私优先** — 清洗过程中产生的所有日志均不得包含患者姓名、ID或联系方式
2. **不修改原始数据** — 所有操作在副本上执行，原始文件存档
3. **业务规则须临床确认** — `references/cleaning-rules.md` 中的参考范围在大规模应用前须经临床专家审核
4. **迭代清洗** — 首次清洗后与业务方对齐，通常需要3–5轮才能达到可用质量

---

## 许可证

MIT — 可自由使用、修改与分发。
