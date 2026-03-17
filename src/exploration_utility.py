from src import paths


def get_full_filename(filename):
    return f"{paths.EXPLORATION_OUTPUT_DIR}/{filename}"


def combine_lines(*lines):
    filtered_lines = [line for line in lines if line is not None]
    return "\n".join(filtered_lines)


def remap(value, in_min_max, out_min_max):
    return out_min_max[0] + (value - in_min_max[0]) * (out_min_max[1] - out_min_max[0]) / (in_min_max[1] - in_min_max[0])
