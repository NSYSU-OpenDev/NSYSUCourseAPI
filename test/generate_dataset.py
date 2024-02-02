import asyncio
from pathlib import Path
import shutil
import time
import aiohttp
import io

import torch
import numpy as np
from PIL import Image

from model import make_model

BASEURL = "https://selcrs.nsysu.edu.tw/menu1"
MODULE_PATH = "./model/EfficientCapsNet.pth"


def parse_valid_code(img: bytes):
    # Load the image
    image = Image.open(io.BytesIO(img))

    # Convert the image to grayscale
    image = image.convert("L")

    # Get the width of the image
    width = image.size[0]

    # Determine the size of each slice
    slice_width = width // 4

    # Create a list to hold the image slices
    slices = []

    # Slice the image and resize each slice
    for i in range(4):
        slice_img = image.crop(
            (i * slice_width, 0, (i + 1) * slice_width, image.size[1])
        )
        slice_img = slice_img.resize((28, 28))
        slices.append(slice_img)

    # Determine the device to use
    device = torch.device("cuda" if torch.cuda.is_available() else "cpu")

    # Convert the slices to a NumPy array
    slices = np.array([np.array(slice_img) for slice_img in slices])

    background_color = np.argmax(np.bincount(slices.flatten()))
    slices = abs(slices - background_color) > 10

    # Load the model
    model = make_model()

    # Load the model weights
    model.load_state_dict(torch.load(MODULE_PATH)["model_state_dict"])
    model.to(device)

    # Use the model to make predictions
    slices_tensor = torch.tensor(slices, dtype=torch.float32).view(4, 1, 28, 28)
    slices_tensor = slices_tensor.to(device)  # Move the slices tensor to the correct device
    _, predictions = model.forward(slices_tensor)

    # Get the predicted classes
    predicted_classes: np.ndarray = torch.argmax(predictions, dim=1).cpu().numpy()

    return "".join(map(str, predicted_classes))


async def fetch(s: aiohttp.ClientSession, code: str):
    async with s.post(
        f"{BASEURL}/dplycourse.asp?page={1}",
        data={
            "HIS": "",
            "IDNO": "",
            "ITEM": "",
            "D0": "1122",
            "DEG_COD": "*",
            "D1": "",
            "D2": "",
            "CLASS_COD": "",
            "SECT_COD": "",
            "TYP": "1",
            "SDG_COD": "",
            "teacher": "",
            "crsname": "",
            "T3": "",
            "WKDAY": "",
            "SECT": "",
            "nowhis": "1",
            "ValidCode": code,
        },
    ) as resp:
        return await resp.text()


async def main():
    images = Path("images")

    if images.is_dir():
        shutil.rmtree(str(images))

    total = done_count = error_count = 0
    async with aiohttp.ClientSession() as s:
        while total < 1000:
            out = await s.get(
                f"{BASEURL}/validcode.asp?epoch={time.time()}",
            )

            img = await out.read()
            code = parse_valid_code(img)
            out = await fetch(s, code)
            total += 1
            if "Wrong Validation Code" in out:
                error_count += 1
                path = images / f"/errors/{error_count}_{code}.png"
                path.parent.mkdir(parents=True, exist_ok=True)
                with path.open("wb") as f:
                    f.write(img)
            else:
                done_count += 1
                path = images / f"/done/{done_count}_{code}.png"
                path.parent.mkdir(parents=True, exist_ok=True)
                with path.open("wb") as f:
                    f.write(img)

            print(
                f"total: {total:04d}, done: {done_count:04d}, error: {error_count:04d}"
                " [{:.2f}%]".format(done_count / total * 100)
            )


asyncio.run(main())
