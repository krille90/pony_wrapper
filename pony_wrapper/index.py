import pony.orm


class Index:
    def __init__(self, attr_names, **kwargs):
        self.attr_names = attr_names
        self.kwargs = kwargs
        self.is_pk = self.kwargs.get('is_pk', False)

    def convert(self, attr_dict):
        attrs = [attr_dict[attr_name] for attr_name in self.attr_names]

        if self.is_pk:
            for i, attr in enumerate(attrs):
                attr.is_part_of_unique_index = True
                attr.composite_keys.append((attrs, i))

        return pony.orm.core.Index(*attrs, **self.kwargs)
