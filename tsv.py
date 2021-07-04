def tsv_get_field(data, field):
    for i, part in enumerate(field):
        if part == "[]":
            return ",".join(tsv_get_field(d, field[i + 1 :]) for d in data)
        elif part in data:
            data = data[part]
        else:
            return ""

    if isinstance(data, bool):
        return str(data).upper()
    else:
        return str(data)


def export_tsv(args):
    with open(args.tsv) as f:
        fields = list(filter(None, map(lambda l: l.strip(), f.readlines())))

    with open(os.path.splitext(args.output)[0] + ".tsv", "w") as f:
        f.write("\t".join(fields) + "\n")
        fields = list(
            map(lambda l: l.replace(".", " ").replace("[]", " []").split(), fields)
        )
        for data in metadata:
            f.write("\t".join(tsv_get_field(data, field) for field in fields) + "\n")
