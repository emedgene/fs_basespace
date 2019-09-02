from abc import abstractmethod
from fs import errors


class classproperty:
    def __init__(self, getter):
        self.getter = getter

    def __get__(self, instance, owner):
        return self.getter(owner)


class EntityContextMeta(type):
    def __new__(cls, name, inheritance_tuple, attr, **kwargs):
        cls_obj = super().__new__(cls, name, inheritance_tuple, attr)
        cls_obj.CATEGORY_MAP = {}
        for category in kwargs.get("categories", []):
            cls_obj.CATEGORY_MAP[category.NAME] = category
        return cls_obj


class EntityContext(metaclass=EntityContextMeta):
    def __init__(self, raw_obj):
        self.raw_obj = raw_obj

    def list(self, api):
        return [context(self.raw_obj) for context in self.CATEGORY_MAP.values()]

    def get(self, api, category):
        return self.CATEGORY_MAP[category](self.raw_obj)

    def get_name(self):
        return self.raw_obj.Name

    def get_id(self):
        return self.raw_obj.Id

    def get_raw(self):
        return self.raw_obj


class CategoryContext:
    NAME = "undefined"
    ENTITY_CONTEXT = None

    def __init__(self, raw_obj):
        self.raw_obj = raw_obj

    @abstractmethod
    def list_raw(self, api):
        raise NotImplementedError("Should return list of entity contexts")

    def list(self, api):
        return [self.ENTITY_CONTEXT(entity) for entity in self.list_raw(api)]

    @abstractmethod
    def get_raw(self, api, entity_id):
        raise NotImplementedError("Should return ENTITY_CONTEXT instance by id")

    def get(self, api, entity_id):
        return self.ENTITY_CONTEXT(self.get_raw(api, entity_id))

    def get_name(self):
        return self.NAME

    def get_id(self):
        return self.NAME


class CategoryContextDirect(CategoryContext):
    def get(self, api, entity_id):
        return self.get_entity_by_id(api, entity_id)

    @classmethod
    @abstractmethod
    def get_raw_entity_direct(cls, api, entity_id):
        raise NotImplementedError("Should return entity context by id")

    @classmethod
    def get_entity_direct(cls, api, entity_id):
        return cls.ENTITY_CONTEXT(cls.get_raw_entity_direct(api, entity_id))


class FileContext(EntityContext):
    def list_raw(self, api):
        raise TypeError("list_raw() is not applicable to a single file")

    def get_raw(self, api, file_id):
        raise TypeError("get_raw() is not applicable to a single file")


class FileGroupContext(CategoryContextDirect):
    NAME = "files"
    ENTITY_CONTEXT = FileContext

    def list_raw(self, api):
        return self.raw_obj.getFiles(api)

    @classmethod
    def get_raw_entity_direct(cls, api, file_id):
        return api.getFileById(file_id)


class FileGroupsContext(EntityContext, categories=[FileGroupContext]):
    pass


class AppResultsContext(CategoryContextDirect):
    NAME = "appresults"
    ENTITY_CONTEXT = FileGroupsContext

    def list_raw(self, api):
        return self.raw_obj.getAppResults(api)

    @classmethod
    def get_raw_entity_direct(cls, api, result_id):
        return api.getAppResultById(result_id)


class SamplesContext(CategoryContextDirect):
    NAME = "samples"
    ENTITY_CONTEXT = FileGroupsContext

    def list(self, api):
        return self.raw_obj.getSamples(api)

    @classmethod
    def get_raw_entity_direct(cls, api, sample_id):
        return api.getSampleById(sample_id)


class ProjectContext(EntityContext, categories=[AppResultsContext,
                                                          SamplesContext]):
    pass


class ProjectGroupContext(CategoryContextDirect):
    NAME = "projects"
    ENTITY_CONTEXT = ProjectContext

    def list_raw(self, api):
        return api.getProjectByUser()

    @classmethod
    def get_raw_entity_direct(cls, api, project_id):
        return api.getProjectById(project_id)


class UserContext(EntityContext, categories=[ProjectGroupContext]):
    pass


def get_context_by_key_abstraction(self, key):
    current_context = UserContext(self.basespace.getUserById('current'))
    if key == "":
        return current_context
    for tag in key.split("/"):
        try:
            current_context = current_context.get(self.basespace, tag)
        except KeyError:
            raise errors.ResourceNotFound
    return current_context


# Breaks the abstraction but creates an average of 2X performance improvements
def get_context_by_key(api, key):
    if not key or key == '/':
        return UserContext(None)
    paths = key.split("/")
    if paths[0] not in UserContext.CATEGORY_MAP:
        raise errors.ResourceNotFound
    if len(paths) == 1:
        user = UserContext(api.getUserById('current'))
        return UserContext(user).get(api, paths[0])
    project_id = paths[1]
    if len(paths) == 2:
        return ProjectContext(api.getProjectById(project_id))
    if paths[2] not in ProjectContext.CATEGORY_MAP:
        raise errors.ResourceNotFound
    if len(paths) == 3:
        project = ProjectContext(api.getProjectById(project_id))
        return project.get(api, paths[2])
    if len(paths) == 4 and paths[2] == SamplesContext.NAME:
        return SamplesContext.get_entity_direct(api, paths[3])
    if len(paths) == 4 and paths[2] == AppResultsContext.NAME:
        return AppResultsContext.get_entity_direct(api, paths[3])
    if len(paths) == 6:
        return FileGroupContext.get_entity_direct(api, paths[5])
    raise errors.ResourceNotFound
