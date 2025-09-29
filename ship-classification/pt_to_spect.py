import torch
import matplotlib.pyplot as plt

# Load your feature tensor
path = "/Users/catherinebertozzi/hackathon-datasets/ship-classification/data/oceanship/20210101T000214.451Z_4_id_5_typecargo_60.pt"  # change this
data = torch.load(path, map_location="cpu")

# Remove singleton dimensions: (1, 3, 1, 128, 204) -> (3, 128, 204)
tensor = data.squeeze()

print("Tensor shape after squeeze:", tensor.shape)  # should be (3, 128, 204)

# Visualize each channel separately
fig, axs = plt.subplots(1, tensor.shape[0], figsize=(15, 5))

for i in range(tensor.shape[0]):
    axs[i].imshow(tensor[i], aspect="auto", origin="lower")
    axs[i].set_title(f"Channel {i}")
    axs[i].set_xlabel("Time frames")
    axs[i].set_ylabel("Mel bins")

plt.tight_layout()
plt.show()
