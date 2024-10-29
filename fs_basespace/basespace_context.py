import re
from abc import abstractmethod
from typing import Tuple

from fs import errors
from fs_basespace.api_factory import BasespaceApiFactory


Page = Tuple[int, int]


class classproperty:
    def __init__(self, getter):
        self.getter = getter

    def __get__(self, instance, owner):
        return self.getter(owner)


class EntityContextMeta(type):
    def __new__(mcs, name, inheritance_tuple, attr, **kwargs):
        cls_obj = super().__new__(mcs, name, inheritance_tuple, attr)
        cls_obj.CATEGORY_MAP = {}
        for category in kwargs.get("categories", []):
            cls_obj.CATEGORY_MAP[category.NAME] = category
        return cls_obj


class EntityContext(metaclass=EntityContextMeta):
    def __init__(self, raw_obj):
        self.raw_obj = raw_obj

    def list(self, api: BasespaceApiFactory, page: Page):
        return [context(self.raw_obj) for context in self.CATEGORY_MAP.values()]

    def get(self, api: BasespaceApiFactory, category):
        return self.CATEGORY_MAP[category](self.raw_obj)

    @classmethod
    def get_lazy(cls, category):
        return cls.CATEGORY_MAP[category]

    def get_name(self):
        return getattr(self.raw_obj, 'Name', getattr(self.raw_obj, 'name', getattr(self.raw_obj, 'bio_sample_name', None)))

    def get_id(self):
        return getattr(self.raw_obj, 'Id', getattr(self.raw_obj, 'id', None))

    def get_date_created(self):
        return getattr(self.raw_obj, 'DateCreated', getattr(self.raw_obj, 'date_created', None))

    def get_size(self):
        return getattr(self.raw_obj, 'Size', getattr(self.raw_obj, 'size', None))


class CategoryContext:
    NAME = "undefined"
    ENTITY_ID_FORMAT = re.compile("^[0-9]+$")
    ENTITY_CONTEXT = None

    def __init__(self, raw_obj):
        self.raw_obj = raw_obj

    @abstractmethod
    def list_raw(self, api: BasespaceApiFactory, page: Page):
        raise NotImplementedError("Should return list of entity contexts")

    def list(self, api: BasespaceApiFactory, page: Page):
        return [self.ENTITY_CONTEXT(entity) for entity in self.list_raw(api, page)]

    @abstractmethod
    def get_raw(self, api: BasespaceApiFactory, entity_id):
        raise NotImplementedError("Should return ENTITY_CONTEXT instance by id")

    def get(self, api: BasespaceApiFactory, entity_id):
        return self.ENTITY_CONTEXT(self.get_raw(api, entity_id))

    @classmethod
    def get_lazy(cls, entity_id):
        if not cls.validate_entity_id(entity_id):
            raise ValueError("Invalid entity id")
        return cls.ENTITY_CONTEXT

    @classmethod
    def validate_entity_id(cls, entity_id):
        match = cls.ENTITY_ID_FORMAT.match(entity_id)
        return bool(match)

    def get_name(self):
        return self.NAME

    def get_id(self):
        return self.NAME


class CategoryContextDirect(CategoryContext):
    def get_raw(self, api: BasespaceApiFactory, entity_id):
        return self.get_raw_entity_direct(api, entity_id)

    @classmethod
    def get_entity_direct(cls, api: BasespaceApiFactory, entity_id: str, page: Page):
        return cls.ENTITY_CONTEXT(cls.get_raw_entity_direct(api, entity_id, page))

    @classmethod
    @abstractmethod
    def get_raw_entity_direct(cls, api: BasespaceApiFactory, entity_id: str, page: Page):
        raise NotImplementedError("Should return entity context by id")


class FileContext(EntityContext):
    def list_raw(self, api: BasespaceApiFactory, page: Page):
        raise TypeError("list_raw() is not applicable to a single file")

    @classmethod
    def get_lazy(cls, category):
        return AfterFileContext

    def get(self, api: BasespaceApiFactory, category):
        return self


class AfterFileContext(EntityContext):
    def __init__(self, raw_obj):
        super().__init__(raw_obj)
        raise NotImplemented("After file context only allow one path.")


class FileGroupContext(CategoryContextDirect):
    NAME = "files"
    ENTITY_CONTEXT = FileContext

    def list_raw(self, api: BasespaceApiFactory, page: Page):
        return self.raw_obj.getFiles(api.base_api)

    @classmethod
    def get_raw_entity_direct(cls, api: BasespaceApiFactory, file_id: str, page: Page):
        return api.base_api.getFileById(file_id)


class FileGroupsContext(EntityContext, categories=[FileGroupContext]):
    pass


class AppResultsContext(CategoryContextDirect):
    NAME = "appresults"
    ENTITY_CONTEXT = FileGroupsContext

    def list_raw(self, api: BasespaceApiFactory, page: Page):
        return self.raw_obj.getAppResults(api.base_api)

    @classmethod
    def get_raw_entity_direct(cls, api: BasespaceApiFactory, result_id: str, page: Page):
        return api.base_api.getAppResultById(result_id)


class SamplesContext(CategoryContextDirect):
    NAME = "samples"
    ENTITY_CONTEXT = FileGroupsContext

    def list_raw(self, api: BasespaceApiFactory, page: Page):
        return self.raw_obj.getSamples(api.base_api)

    @classmethod
    def get_raw_entity_direct(cls, api: BasespaceApiFactory, sample_id: str, page: Page):
        return api.base_api.getSampleById(sample_id)


class SequencedFileGroupContext(CategoryContextDirect):
    NAME = "sequenced files"
    ENTITY_CONTEXT = FileContext

    def list_raw(self, api: BasespaceApiFactory, page: Page):
        return self.raw_obj.items

    @classmethod
    def get_raw_entity_direct(cls, api: BasespaceApiFactory, file_id: str, page: Page):
        return api.base_api.getFileById(file_id)


class SequencedFileGroupsContext(EntityContext, categories=[SequencedFileGroupContext]):
    pass


class DatasetsContext(CategoryContextDirect):
    ENTITY_ID_FORMAT = re.compile("^ds.[0-9a-z]+$")
    NAME = "datasets"
    ENTITY_CONTEXT = SequencedFileGroupsContext

    def list_raw(self, api: BasespaceApiFactory, page: Page):
        return self.raw_obj.items

    @classmethod
    def get_raw_entity_direct(cls, api: BasespaceApiFactory, dataset_id: str, page: Page):
        offset, limit = translate_page_to_offset_and_limit(page)
        return api.v2.get_v2_datasets_id_files(excludevcfindexfolder=False,
                                               excludebamcoveragefolder=False,
                                               excludesystemfolder=False,
                                               excludeemptyfiles=False,
                                               filehrefcontentresolution=False,
                                               turbomode=False,
                                               id=dataset_id,
                                               sortdir='Asc',
                                               sortby='Name',
                                               offset=offset,
                                               limit=limit)


class AppSessionContext(EntityContext, categories=[DatasetsContext]):
    pass


class BioSampleContext(EntityContext, categories=[DatasetsContext]):
    pass


class BioSampleGroupContext(CategoryContextDirect):
    NAME = "biosamples"
    ENTITY_CONTEXT = BioSampleContext

    def list_raw(self, api: BasespaceApiFactory, page: Page):
        offset, limit = translate_page_to_offset_and_limit(page)
        params = {'projectid': [self.raw_obj.Id], 'sortby': 'Name', 'offset': offset, 'limit': limit}
        bio_sample_list = api.v2.get_v2_biosamples(**params).items
        return bio_sample_list

    @classmethod
    def get_raw_entity_direct(cls, api: BasespaceApiFactory, biosample_id: str, page: Page):
        offset, limit = translate_page_to_offset_and_limit(page)
        return api.v2.get_v2_datasets(offset=offset,
                                      limit=limit,
                                      sortby='Name',
                                      sortdir='Asc',
                                      include=["properties"],
                                      datasettypes=["~common.fastq"],
                                      propertyfilters=["Input.Libraries", "Input.Runs", "BaseSpace.Metrics.FastQ"],
                                      inputbiosamples=[biosample_id])


class AppSessionsContext(CategoryContextDirect):
    NAME = "appsessions"
    ENTITY_CONTEXT = AppSessionContext

    def list_raw(self, api: BasespaceApiFactory, page: Page):
        offset, limit = translate_page_to_offset_and_limit(page)
        params = {'sortby': 'Name', 'offset': offset, 'limit': limit, 'output_projects': [self.raw_obj.Id]}
        return api.v2.get_v2_appsessions(**params).items

    @classmethod
    def get_raw_entity_direct(cls, api: BasespaceApiFactory, result_id: str, page: Page):
        offset, limit = translate_page_to_offset_and_limit(page)
        return api.v2.get_v2_datasets(offset=offset, limit=limit, appsessionids=[result_id]).items


class ProjectContext(EntityContext, categories=[AppResultsContext,
                                                SamplesContext,
                                                BioSampleGroupContext,
                                                AppSessionsContext]):
    pass


class ProjectGroupContext(CategoryContextDirect):
    NAME = "projects"
    ENTITY_CONTEXT = ProjectContext

    def list_raw(self, api, page: Page):
        return api.base_api.getProjectByUser()

    @classmethod
    def get_raw_entity_direct(cls, api: BasespaceApiFactory, project_id: str, page: Page):
        return api.base_api.getProjectById(project_id)


class UserContext(EntityContext, categories=[ProjectGroupContext]):
    pass


def get_context_by_key_abstraction(self, key):
    current_context = UserContext(self.basespace.base_api.getUserById('current'))
    if key == "":
        return current_context
    for tag in key.split("/"):
        try:
            current_context = current_context.get(self.basespace, tag)
        except KeyError:
            raise errors.ResourceNotFound
    return current_context


ROOT_CONTEXT = UserContext


def get_last_direct_context(key):
    latest_direct = None

    if not key or key == '/':
        return latest_direct

    current_context = ROOT_CONTEXT
    path_steps = key.split("/")
    for i, path_step in enumerate(path_steps):
        if issubclass(current_context, CategoryContextDirect):
            latest_direct = (current_context, "/".join(path_steps[i:]))
        current_context = current_context.get_lazy(path_step)
    return latest_direct


def get_context_by_key(api: BasespaceApiFactory, key: str, page: Page):
    rest_steps = key.split("/") if key else []
    latest_context = ROOT_CONTEXT(None)
    latest_direct = get_last_direct_context(key)
    if latest_direct is not None:
        latest_context_cls, rest_path = latest_direct
        path_steps = rest_path.split("/")
        latest_context = latest_context_cls.get_entity_direct(api, path_steps[0], page)
        rest_steps = path_steps[1:]
    for path_step in rest_steps:
        latest_context = latest_context.get(api, path_step)
    return latest_context


def translate_page_to_offset_and_limit(page: Page):
    offset, limit = 0, 50
    if page:
        offset, offset_end = page
        limit = offset_end - offset
    return offset, limit
