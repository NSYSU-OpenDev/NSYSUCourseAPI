# NSYSUCourseAPI

## API Document

[API Document Website](https://whats2000.github.io/NSYSUCourseAPI/index.html)

## Run

### 開始

```sh
python main.py start
```

### 測試生成資料集

```sh
python main.py test
```

# Docs

<!-- 
│
├ 📂
└
 -->

## API Path

```yml
/                     # 根目錄
├ 📂 [Academic Year]
│ ├ 📂 [Updated]
│ │ ├ all.json
│ │ ├ page-{index}.json
│ │ ├ info.json
│ │ ├ diff.txt
│ │ └ path.json
│ ├ version.json
│ └ path.json
├ version.json
└ path.json
```

## API File Structure

> `📜` 為物件結構
>
> `📄` 為檔案結構。

### 📜 `#course`

| FIELD                | TYPE        | DESCRIPTION                     |
| -------------------- | ----------- | ------------------------------- |
| `id`                 | `string`    | 課號                            |
| `url`                | `string`    | 科目詳細說明網址                |
| `change`             | `?string`   | 更改類別 (異動/新增)            |
| `changeDescription`  | `?string`   | 更改說明                        |
| `multipleCompulsory` | `bool`      | 是否為多門必修                  |
| `department`         | `string`    | 系所別                          |
| `grade`              | `string`    | 年級                            |
| `class`              | `?string`   | 班別                            |
| `name`               | `string`    | 科目名稱 <中文\n英文>           |
| `credit`             | `string`    | 學分                            |
| `yearSemester`       | `string`    | 學年期 [年期]                   |
| `compulsory`         | `bool`      | 是否為必修(若為 false 則為選修) |
| `restrict`           | `int`       | 限修                            |
| `select`             | `int`       | 點選                            |
| `selected`           | `int`       | 選上                            |
| `remaining`          | `int`       | 餘額                            |
| `teacher`            | `string`    | 授課教師                        |
| `room`               | `string`    | 教室                            |
| `classTime`          | `string[7]` | 上課時間[一二三四五六日]        |
| `description`        | `string`    | 備註                            |
| `tags`               | `string[]`  | 標籤                            |
| `english`            | `bool`      | 是否為英語授課                  |

```json
{
  "url": "https://selcrs.nsysu.edu.tw/menu5/showoutline.asp?SYEAR=113&SEM=1&CrsDat=STP101&Crsname=教育心理學",
  "change": "新增",
  "changeDescription": "7/15",
  "multipleCompulsory": false,
  "department": "中學學程",
  "id": "STP101",
  "grade": "0",
  "class": "不分班",
  "name": "教育心理學\nEDUCATIONAL PSYCHOLOGY",
  "credit": "2",
  "yearSemester": "期",
  "compulsory": false,
  "restrict": 50,
  "select": 0,
  "selected": 37,
  "remaining": 13,
  "teacher": "馮雅群",
  "room": "三5,6(社SS 2001)",
  "classTime": [
    "",
    "",
    "56",
    "",
    "",
    "",
    ""
  ],
  "description": "《講授類》\n本課程為教育學程課程",
  "tags": [],
  "english": false
}
```

### 📜 `#path`

<details>
  <summary>File</summary>

  ```json
  {
    "name": "all.json",
    "path": "1122/20240208/all.json",
    "sha256": "f4592e5e23fa54ca89e10fa4528baeed3adc423a015e54a4f3b24a5520ca297c",
    "size": 2000,
    "static_url": "https://whats2000.github.io/NSYSUCourseAPI/1122/20240208/all.json",
    "type": "file",
  }
  ```

</details>

<details>
  <summary>Dir</summary>

```json
{
  "name": "20240208",
  "path": "1122/20240208",
  "static_url": "https://whats2000.github.io/NSYSUCourseAPI/1122/20240208",
  "type": "dir",
}
```

</details>

### 📄 `path.json`

```json
[
  "<#path-dir>",
  "<#path-file>"
]
```

### 📄 `version.json`

<details>
  <summary>Root Path</summary>

| FIELD     | TYPE             | DESCRIPTION                                                          |
| --------- | ---------------- | -------------------------------------------------------------------- |
| `latest`  | `string`         | 最新學年度(和路徑名相同)                                             |
| `history` | `dict[str, str]` | 歷史紀錄 `{ [學年度: str]: [中文標記: str] }` ({學年度}和路徑名相同) |

  ```json
  {
    "latest": "1123",
    "history": {
      "1123": "112暑期",
      "1122": "112下",
      "1121": "112上",
      "1113": "111暑期",
      "1112": "111下",
    }
  }
  ```

</details>

<details>
  <summary>Academic Year</summary>

| FIELD     | TYPE             | DESCRIPTION                                                   |
| --------- | ---------------- | ------------------------------------------------------------- |
| `latest`  | `string`         | 最新版本(和路徑名相同)                                        |
| `history` | `dict[str, str]` | 歷史紀錄 `{ [更新時間(與路徑名相同): str]: [ISO 8601: str] }` |

  ```json
  {
    "latest": "20240408_010805",
    "history": {
      "20240408_010805": "2024-04-08T01:08:05Z",
      "20240407_111005": "2024-04-07T11:10:05Z",
      "20240406_153005": "2024-04-06T15:30:05Z",
      "20240405_204005": "2024-04-05T20:40:05Z",
    }
  }
  ```

</details>

### 📄 `info.json`

| FIELD       | TYPE        | DESCRIPTION                    |
| ----------- | ----------- | ------------------------------ |
| `page_size` | `int`       | page-{index} 中的 index 最大值 |
| `updated`   | `date_time` | 更新時間                       |

```json
{
  "page_size": 20,
  "updated": "20240405_204005"
}
```

### 📄 `all.json` or `page-{index}.json`

> page-{index} 中的 index 從 1~{page_size}
> `page_size` 從 [info.json](#📄-infojson) 中獲取

```json
[
  "<#course>"
]
```
