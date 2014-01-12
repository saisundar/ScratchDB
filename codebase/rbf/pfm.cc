#include "pfm.h"

PagedFileManager* PagedFileManager::_pf_manager = 0;


PagedFileManager* PagedFileManager::Instance()
{
    if(!_pf_manager)
        _pf_manager = new PagedFileManager();

    return _pf_manager;
}


PagedFileManager::PagedFileManager()
{
}


PagedFileManager::~PagedFileManager()
{
}


RC PagedFileManager::CreateFile(const char *fileName)
{
    return -1;
}


RC PagedFileManager::DestroyFile(const char *fileName)
{
    return -1;
}


RC PagedFileManager::OpenFile(const char *fileName, FileHandle &fileHandle)
{
    return -1;
}


RC PagedFileManager::CloseFile(FileHandle &fileHandle)
{
    return -1;
}


FileHandle::FileHandle()
{
}


FileHandle::~FileHandle()
{
}


RC FileHandle::ReadPage(PageNum pageNum, void *data)
{
    return -1;
}


RC FileHandle::WritePage(PageNum pageNum, const void *data)
{
    return -1;
}


RC FileHandle::AppendPage(const void *data)
{
    return -1;
}


unsigned FileHandle::GetNumberOfPages()
{
    return -1;
}


