from abc import abstractmethod, abstractproperty


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


class SamplesContext(CategoryContext):
    NAME = "samples"

    def list(self, api):
        results = self.raw_obj.getSamples(api)
        return [FileGroupContext(result) for result in results]

    def get(self, api, sample_id):
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
