from abc import abstractmethod, abstractproperty

from fs import errors


class classproperty:
    def __init__(self, getter):
        self.getter = getter

    def __get__(self, instance, owner):
        return self.getter(owner)


class EntityContext:
    def __init__(self, raw_obj):
        self.raw_obj = raw_obj

    @abstractproperty
    @classproperty
    def CATEGORY_MAP(cls):
        raise NotImplementedError("Should return dict. Directory name -> corresponding context")

    def list(self, api):
        return [context(api, self.raw_obj) for context in self.CATEGORY_MAP.values()]

    def get(self, api, category):
        return self.CATEGORY_MAP[category](api, self.raw_obj)

    def get_name(self):
        return self.raw_obj.Name

    def get_id(self):
        return self.raw_obj.Id

    def get_raw(self):
        return self.raw_obj


class CategoryContext:
    NAME = "undefined"

    def __init__(self, raw_obj):
        self.raw_obj = raw_obj

    @abstractmethod
    def list(self, api):
        raise NotImplementedError("Should return list of category contexts")

    @abstractmethod
    def get(self, api, entity_id):
        raise NotImplementedError("Should return category context by id")

    def get_name(self):
        return self.NAME

    def get_id(self):
        return self.NAME

    def get_raw(self):
        return self.raw_obj


class FileContext(EntityContext):
    def list(self, api):
        raise TypeError("list() is not applicable to a single file")

    def get(self, api, file_id):
        raise TypeError("get() is not applicable to a single file")


class FileGroupContext(EntityContext):
    def list(self, api):
        files = self.raw_obj.getFiles(api)
        return [FileContext(f) for f in files]

    def get(self, api, file_id):
        return FileContext(api.getFileById(file_id))


class AppResultsContext(CategoryContext):
    NAME = "appresults"

    def list(self, api):
        results = self.raw_obj.getAppResults(api)
        return [FileGroupContext(result) for result in results]

    def get(self, api, result_id):
        return FileGroupContext(api.getAppResultById(result_id))

    @staticmethod
    def get_context_by_id(api, result_id):
        return FileGroupContext(api.getAppResultById(result_id))

class SamplesContext(CategoryContext):
    NAME = "samples"

    def list(self, api):
        results = self.raw_obj.getSamples(api)
        return [FileGroupContext(result) for result in results]

    def get(self, api, sample_id):
        return self.get_context_by_id(api, sample_id)

    @staticmethod
    def get_context_by_id(api, sample_id):
        return FileGroupContext(api.getSampleById(sample_id))


class ProjectContext(EntityContext):
    @staticmethod
    def _get_results(api, raw_project):
        return AppResultsContext(raw_project)

    @staticmethod
    def _get_samples(api, raw_project):
        return SamplesContext(raw_project)

    @classproperty
    def CATEGORY_MAP(cls):
        return {
            AppResultsContext.NAME: cls._get_results,
            SamplesContext.NAME: cls._get_samples
        }


class ProjectGroupContext(CategoryContext):
    NAME = "projects"

    def list(self, api):
        projects = api.getProjectByUser()
        return [ProjectContext(project) for project in projects]

    def get(self, api, project_id):
        return ProjectContext(api.getProjectById(project_id))


class UserContext(EntityContext):
    @staticmethod
    def _get_projects(api, user):
        return ProjectGroupContext(user)

    @classproperty
    def CATEGORY_MAP(cls):
        return {
            ProjectGroupContext.NAME: cls._get_projects
        }
 

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
        return SamplesContext.get_context_by_id(api, paths[3])
    if len(paths) == 4 and paths[2] == AppResultsContext.NAME:
        return AppResultsContext.get_context_by_id(api, paths[3])
    if len(paths) == 5:
        return FileContext(api.getFileById(paths[4]))
    raise errors.ResourceNotFound
