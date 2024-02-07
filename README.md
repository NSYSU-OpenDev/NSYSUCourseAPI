# NSYSUCourseAPI

# Docs

<!-- 
│
├ 📂
└
 -->

## API Path

```yml
/                     # 根目錄
├ 📂 [School Year]
│ ├ 📂 [Updated]
│ │ ├ all.json
│ │ ├ page-{index}.json
│ │ ├ info.json
│ │ └ path.json
│ └ path.json
├ version.json
└ path.json
```

## API File Structure

`📜` is object structure，`📄` is file structure.

### 📜 `#course`

| FIELD              | TYPE      | DESCRIPTION                     |
| ------------------ | --------- | ------------------------------- |
| id                 | string    | 課號                            |
| url                | string    | 科目詳細說明網址                |
| change             | ?string   | 更改類別 (異動/新增)            |
| changeDescription  | ?string   | 更改說明                        |
| multipleCompulsory | bool      | 是否為多門必修                  |
| department         | string    | 系所別                          |
| grade              | string    | 年級                            |
| class              | ?string   | 班別                            |
| name               | string    | 科目名稱 <中文\n英文>           |
| credit             | string    | 學分                            |
| yearSemester       | string    | 學年期 [年期]                   |
| compulsory         | bool      | 是否為必修(若為 false 則為選修) |
| restrict           | int       | 限修                            |
| select             | int       | 點選                            |
| selected           | int       | 選上                            |
| remaining          | int       | 餘額                            |
| teacher            | string    | 授課教師                        |
| room               | string    | 教室                            |
| classTime          | string[7] | 上課時間[一二三四五六日]        |
| description        | string    | 備註                            |
| tags               | string[]  | 標籤                            |
| english            | bool      | 是否為英語授課                  |

```json
{
  "url": "https://selcrs.nsysu.edu.tw/menu5/showoutline.asp?SYEAR=112&SEM=2&CrsDat=GECE104D&Crsname=中文思辨與表達（二）",
  "change": "異動",
  "changeDescription": "更換教師1/16",
  "multipleCompulsory": "*",
  "department": "中文思辨與表達",
  "number": "GECE104D",
  "grade": "0",
  "class": "不分班",
  "name": "中文思辨與表達（二）\nCRITICAL THINKING AND EXPRESSION IN CHINESE（II）",
  "credit": "3",
  "yearSemester": "期",
  "compulsory": true,
  "restrict": 45,
  "select": 9,
  "selected": 21,
  "remaining": 24,
  "teacher": "林芷瑩",
  "room": "理SC 0014",
  "classTime": ["", "234", "", "", "", "", ""],
  "description": "《講授類》\n1、適用111學年度(含)以後入學: 中文思辨與表達(一)、(二)課程，於畢業前僅修習其中1門即可。 2、本課程「加退選期間」額滿不予加簽。",
  "tags": ["中文創意跨域應用整合學程"],
  "english": false
}
```

### 📄 `path.json`

### 📄 `version.json`

### 📄 `info.json`

## Run

### Test Generation Dataset

```sh
python main.py test
```

### Start

```sh
python main.py start
```
