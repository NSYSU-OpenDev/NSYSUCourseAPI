import io
import os

import requests
from bs4 import Tag

from utils.utils import is_integer


def parse_course_info(d: Tag, original_page: str, **kwargs):
    try:
        for line_break in d.find_all("br"):
            line_break: Tag
            line_break.replace_with("\n")

        original_data = list(map(lambda x: x.text.strip(), d))

        assert len(original_data) >= 26, f"len(original_data) = {original_data}"

        (
            change,
            changeDescription,
            multipleCompulsory,
            department,
            id,
            grade,
            _class,
            name,
            credit,
            yearSemester,
            compulsoryElective,
            restrict,
            select,
            selected,
            remaining,
            teacher,
            room,
        ) = original_data[:17]
        classTime = original_data[17:24]

        # get tags
        tags: list[str] = []
        description_el = d.select_one("td:nth-child(25)")
        if description_el:
            for tag in description_el.select("font"):
                tags.append(tag.text)
                tag.extract()
        # get description
        description = description_el.text if description_el else ""

        # check if the course is taught in English
        # Since some descriptions have multiple identical suffixes,
        # use while to check multiple times.
        english = False
        while description.endswith("※英語授課"):
            english = True
            description = description[:-6]
        description = description.strip()

        info_url_el = d.select_one("td:nth-child(8) small a[href]")
        assert info_url_el, "info_url_el is None"
        url = info_url_el.attrs["href"]

        assert change in ["", "異動", "新增"], f"Change = {change}"
        assert multipleCompulsory in " *", f"MultipleCompulsory = {multipleCompulsory}"
        assert grade, f"grade = {grade}"
        assert credit, f"credit = {credit}"
        assert yearSemester in "年期", f"yearSemester = {yearSemester}"
        assert (
            compulsoryElective in "必選"
        ), f"compulsoryElective = {compulsoryElective}"

        assert is_integer(restrict), f"restrict = {restrict}"
        assert is_integer(select), f"select = {select}"
        assert is_integer(selected), f"selected = {selected}"
        assert is_integer(remaining), f"remaining = {remaining}"

        opl_str = lambda x: None if x == "" else x
        return {
            "url": url,
            "change": opl_str(change),
            "changeDescription": opl_str(changeDescription),
            "multipleCompulsory": multipleCompulsory == "*",
            "department": department,
            "id": id,
            "grade": grade,
            "class": opl_str(_class),
            "name": name,
            "credit": credit,
            "yearSemester": yearSemester,
            "compulsory": compulsoryElective == "必",
            "restrict": int(restrict),
            "select": int(select),
            "selected": int(selected),
            "remaining": int(remaining),
            "teacher": teacher,
            "room": room,
            "classTime": classTime,
            "description": description,
            "tags": tags,
            "english": english,
        }
    except AssertionError as e:
        parse_assert_warn(e, original_page, **kwargs)
        return False


def parse_assert_warn(error: AssertionError, original_page: str, **kwargs):
    if os.getenv("NO_WARNING"):
        return

    if webhook := os.getenv("WEBHOOK"):
        try:
            requests.post(
                webhook,
                data={"content": f"parse error: ```{error}```\n{kwargs}"},
                files={"file": ("page.html", io.StringIO(original_page))},
            )
        except Exception as e:
            print(e)

    print(error)
