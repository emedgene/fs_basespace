fs\_basespace
------

pyfilesystem2 interface to Illumina Basespace

format of url string:

```
basespace://{clientKey}:{clientSecret}:{appToken}@{server}
```

accessing projects:

```
basespace://{clientKey}:{clientSecret}:{appToken}@{server}!/projects/{projectId}
```

accessing project sample files:

```
basespace://{clientKey}:{clientSecret}:{appToken}@{server}!/projects/{projectId}/samples/{sampleId}/{fileId}
```

accessing project appResult files (bam, vcf, ...):

```
basespace://{clientKey}:{clientSecret}:{appToken}@{server}!/projects/{projectId}/appresults/{resultId}/{fileId}
```
