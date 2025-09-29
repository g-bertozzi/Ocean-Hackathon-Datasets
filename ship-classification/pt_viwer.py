import torch

def summarize_pt_file(path):
    data = torch.load(path, map_location="cpu")

    if torch.is_tensor(data):
        return {
            "type": "Tensor",
            "shape": tuple(data.shape),
            "dtype": str(data.dtype),
        }

    elif isinstance(data, dict):
        summary = {"type": "Dict", "keys": list(data.keys())[:10]}
        for k, v in data.items():
            if torch.is_tensor(v):
                summary[f"tensor_{k}_shape"] = tuple(v.shape)
                summary[f"tensor_{k}_dtype"] = str(v.dtype)
        return summary

    elif isinstance(data, list):
        summary = {"type": "List", "length": len(data)}
        if len(data) > 0 and torch.is_tensor(data[0]):
            summary["first_tensor_shape"] = tuple(data[0].shape)
            summary["first_tensor_dtype"] = str(data[0].dtype)
        return summary

    else:
        return {"type": str(type(data))}


# ---- Example usage ----
path = "/Users/catherinebertozzi/hackathon-datasets/ship-classification/data/oceanship/20210101T000214.451Z_4_id_5_typecargo_60.pt"

# path = "/Users/catherinebertozzi/hackathon-datasets/ship-classification/data/oceanship/20210101T000214.451Z_4_id_5_typecargo_60_text.pt"  # change to your file
print(summarize_pt_file(path))
