import io

import numpy as np
import torch
from PIL import Image, ImageFilter

from utils.model import make_deploy_model


def parse_valid_code(img: bytes, module_path="model/EfficientCapsNetDeploy.pth"):
    # Load the image
    image = Image.open(io.BytesIO(img))

    # Convert the image to grayscale
    image = image.convert("L")

    # Apply Median Filter to reduce noise
    image = image.filter(ImageFilter.MedianFilter(size=3))

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
        # Resize and apply Median Filter again to ensure consistency after cropping
        slice_img = slice_img.resize((28, 28))
        slices.append(slice_img)

    # Determine the device to use
    device = torch.device("cuda" if torch.cuda.is_available() else "cpu")

    # Convert the slices to a NumPy array and normalize the pixel values
    slices = np.array([np.array(slice_img) / 255.0 for slice_img in slices])

    # Build the model
    model = make_deploy_model()

    # Load the model weights
    model.load_state_dict(torch.load(module_path, map_location=device))
    model.to(device)
    model.eval()

    # Convert slices to a tensor, normalize and add a batch dimension
    slices_tensor = torch.tensor(slices, dtype=torch.float32).unsqueeze(
        1
    )  # Add channel dimension
    slices_tensor = slices_tensor.to(
        device
    )  # Move the slices tensor to the correct device

    with torch.no_grad():
        _, predictions = model(slices_tensor)

    # Get the predicted classes
    predicted_classes = torch.argmax(predictions, dim=1).cpu().numpy() + 1

    return "".join(map(str, predicted_classes))
