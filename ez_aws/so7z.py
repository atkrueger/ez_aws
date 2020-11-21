#extension of py7zr's SevenZipFile class that
#1. lets you open any file you can open with smart_open (such as S3/azure files)
#2. lets you stream-decompress a 7zip file on the fly, so that you can decompress it even if it is larger than computer memory

from __future__ import annotations 
#this import statement lets you specify the type of get_stram before the SmartOpen7zStream class is defined

import py7zr
import smart_open
from typing import IO, Any, BinaryIO, Dict, List, Tuple, Union
import queue
import time

from py7zr.archiveinfo import Folder, Header, SignatureHeader
from py7zr.callbacks import ExtractCallback
from py7zr.compressor import SupportedMethods, get_methods_names_string, SevenZipDecompressor
from py7zr.exceptions import Bad7zFile, CrcError, DecompressionError, InternalError, UnsupportedCompressionMethodError
from py7zr.helpers import ArchiveTimestamp, MemIO, NullIO, calculate_crc32, filetime_to_dt, readlink
from py7zr.properties import ARCHIVE_DEFAULT, ENCRYPTED_ARCHIVE_DEFAULT, MAGIC_7Z, READ_BLOCKSIZE
import os
import io


#from checksum_error import ChecksumIncorrectError

class SmartOpen7z(py7zr.SevenZipFile):
    """Exactly the same as SevenZipFile, but can load from s3 by using smart_open
    smart_open_url must be a url that smart_open accepts
    As with SevenZipFile, you need to create a new instance each time you want to extract from a file
    (Don't worry, creating a new instance only grabs a stream to the start of the file, it doesn't extract it)"""
    def __init__(self, smart_open_url: str  = None, bucket : str = None, key : str = None, mode: str = 'r', *, 
        filters: List[Dict[str,int]] = None,
        dereference=False,
        password: str = None) -> None:
        self.password = password
        self.smart_open_uri = smart_open_url
        if mode not in ('r', 'w'):
            raise ValueError("ZipFile requires mode 'r', 'w'")
        self.password_protected = (password is not None)
        # Check if we were passed a file-like object or not
        #if isinstance(smart_open_url, str):
        if smart_open_url ==None and bucket!= None and key != None:
            smart_open_url = f"S3://{bucket}/{key}"
            self.smart_open_uri = smart_open_url

        if smart_open_url != None:
           
            self._filePassed = False  # type: bool
            self.filename = smart_open_url  # type: str

            if mode == 'r':
                self.fp = smart_open.open(smart_open_url, 'rb')  # type: BinaryIO
            elif mode == 'w':
                self.fp = smart_open.open(smart_open_url, 'wb')
           
            else:
                raise ValueError("File open error.")
            self.mode = mode
        else:
            raise TypeError("invalid file: {}".format(type(smart_open_url)))
        # elif bucket!= None and key != None:
        #     self._filePassed=False
        #     self.filename="S3://" + bucket + '/' + key
        #     if use_buffered_fp:
        #         if mode == 'r':
        #             self.fp = BufferedFp(
        #                 bucket=bucket,
        #                 key=key,
        #             )
        #         else:
        #             raise NotImplementedError("only read mode has been implemented for buffered_fp")
        #     else: #if use_buffered_fp==False
        #         if mode=='r':
        #             print(f"Filename: {self.filename}")
        #             self.fp = smart_open.open(self.filename, mode='rb')
        #         elif mode=='w':
        #             self.fp= smart_open.open(self.filename, 'wb')
        #         else:
        #             raise NotImplementedError
                
        
        self._fileRefCnt = 1
        try:
            if mode == "r":
                self._real_get_contents(password) #this is where the file sizes are determined
                self.fp.seek(self.afterheader)  # seek into start of payload and prepare worker to extract
                self.worker = py7zr.py7zr.Worker(self.files, self.afterheader, self.header)
            elif mode in 'w':
                self._prepare_write(filters, password)
            #elif mode in 'x':
            #    raise NotImplementedError
            #elif mode == 'a':
            #    self._real_get_contents(password)
            #    self._prepare_append(filters, password)
            else:
                raise ValueError("Mode must be 'r' or 'w'")
        except Exception as e:
            self._fpclose()
            raise e
        self.encoded_header_mode = True
        self.header_encryption = False
        self._dict = {}  # type: Dict[str, IO[Any]]
        self.dereference = dereference
        self.reporterd = None  # type: Optional[threading.Thread]
        self.q = queue.Queue()  # type: queue.Queue[Any]

    def get_stream(self, archive_file_name: str, report_progress = False, mode = 'r', report_fp=False) -> SmartOpen7zStream :
        """returns a one-shot SmartOpen7zStream object to so you can stream a particular file"""
        return SmartOpen7zStream(
            smart_open_uri = self.smart_open_uri,
            archive_file_name= archive_file_name,
            password = self.password,
            report_progress = report_progress,
            mode = mode,
            report_fp=report_fp
        )

    def get_stream_portion(self, archive_file_name: str, start_pct: float) ->SmartOpen7zStream:
        """streams a portion of the data, from start_pct to end_pct
        For example, if start_pct = 0.5 and end_pct = 0.75, it will start streaming halfway through the file, and 
        termiante at the 75th percentile"""

        stream =self.get_stream(archive_file_name) #instantaite a new stream
        stream.set_start_pct(start_pct)
        #stream.set_end_pct(end_pct)
        return stream
        


    def decompress_file(self, archive_file_name : str, smart_open_output_url: str, report_progress=False) -> None:
        """decompresses using a stream (so the whole file doesn't need to be stored in memory).

        smart_open_output_url must be in a format that smart_open can read"""
        with smart_open.open(
            uri = smart_open_output_url,
            mode='wb') as fout:
            for chunk in self.get_stream(archive_file_name, report_progress=report_progress):
                fout.write(chunk)

    def decompress_all(self, out_folder_url: str, report_progress=False):
        """decompresses every file in the archive to the specified output folder.

        This uses smart_open.open to create the output files
        Note: may need to implement folder creation for saving locally as opposed to on S3
        Note: should add a parallelization option that async decompresses it"""

        for file in self.files:
            if file.uncompressed==0: #in more complete implementation
                if report_progress:
                    print("Skipping the following file because it is of size 0 bytes: ", file.filename)
                continue #skip to the next
            if report_progress:
                print("Decompressing ", file.filename)
            self.decompress_file(
                archive_file_name = file.filename,
                smart_open_output_url = out_folder_url + "/" + file.filename,
                report_progress=report_progress
            )

    #issue is _check7zfile is returning false
    #basically just checks if MAGIC_7Z == fp.read(len(MAGIC_7Z))[:len(MAGIC_7Z)]
    """
    @staticmethod #override of the py7zr original for debugging
    def _check_7zfile(fp: Union[BinaryIO, io.BufferedReader]) -> bool:
        print(f"MAGIC_7Z: {MAGIC_7Z}")
        output = fp.read(len(MAGIC_7Z))[:len(MAGIC_7Z)]
        result = MAGIC_7Z == output
        print(f"output: {output}")
        fp.seek(-len(MAGIC_7Z), 1)
        return result
    """

   
class SmartOpen7zStream:
    """Stream iterator you are meant to use once, e.g.:
    for chunk in SmartOpen7zStream(uri, mode = 'r', archive_file_name= archive_file_name, password=password):
        print(chunk.decode('utf-8'))"""

    """initialize with a smart open url the name of the archive file you want to stream"""
    
    #def __init__(self, smart_open_uri: str, archiveFile : py7zr.py7zr.ArchiveFile, password: Optional[str] = None, mode='r', *,
    def __init__(self, smart_open_uri: str, archive_file_name : str, password: str = None, mode='r', *,
        filters: List[Dict[str,int]] = None,  dereference=False, report_progress=False, report_fp=False) -> None:
        #if report progress is true, then it will print pct complete, anticipated time remaining
        #if report fp is true, then it will print how much fp has moved with each iteration
        self.smartOpen7z = SmartOpen7z(
            smart_open_url = smart_open_uri,
            mode = mode,
            password=password,
            filters = filters,
            dereference=dereference
        )

        self.archiveFile = self.get_archive_file(archive_file_name) #gets a fresh archive file
        self.crc32=0
        self.pct_complete=0
        self.out_remaining = self.archiveFile.uncompressed
        #self.archiveFile = archiveFile
        #self.decompressor = archiveFile.folder.get_decompressor(archiveFile.compressed)
        #waiting to instantiate at the same time
        self.fp = self.smartOpen7z.fp
        self.report_fp = report_fp

        #move it to the right spot
        if self.out_remaining>0: #if the file is of size 0 ,then there is nothing to decompress, and it shouldn't do anything
            self.move_fp_to_correct_spot()
            self.decompressor = self.archiveFile.folder.get_decompressor(self.archiveFile.compressed)
            if report_fp:
                self.starting_fp_position = self.fp.tell()
        self.report_progress= report_progress
    
    def get_archive_file(self, archive_file_name : str) -> py7zr.py7zr.ArchiveFile:
        for archive_file in self.smartOpen7z.files:
            if archive_file.filename == archive_file_name:
                return archive_file

    def move_fp_to_correct_spot(self):
        #fp = self.smartOpen7z.fp
        folder = self.archiveFile.folder
        worker = self.smartOpen7z.worker

        ### Begin moving fp to the correct spot
        folders = worker.header.main_streams.unpackinfo.folders
        positions = worker.header.main_streams.packinfo.packpositions
        numfolders = worker.header.main_streams.unpackinfo.numfolders

        for i in range(numfolders):
            #list of files in the folder:
            #print("folder " + str(i))
            for f in folders[i].files:
                #print(f.filename)
                if f.filename == self.archiveFile.filename:
                    folder_position = positions[i] #never reached this point
                    #print("proper folder found!")
                
        #print(f"worker.src_start ={worker.src_start}, folderposition = {folder_position}")
        src_start = worker.src_start + folder_position #just need to get positions[i] now
        src_end = worker.src_start + worker.header.main_streams.packinfo.packpositions[-1]
        self.fp.seek(src_start) 
        #print("fp position after fp.seek(src_start) in streamDecompress: " + str(fp.tell()))

        just_check=[] #List[ArchiveFile]
        for f in folder.files: #again, seems like an unnecssary loop if we are extracting one file at a time
            #if f == archiveFile:
            if f.filename == self.archiveFile.filename:
                worker._check(self.fp, just_check, src_end)
                #now it is at the final spot
                just_check = []
            elif not f.emptystream:
                just_check.append(f) #so the other files in ArchiveFileList are being added to just_check to move the fp pointer
        
    def __iter__(self):
        self.start_time = time.time()
        return self

    def __next__(self) -> bytes:
       
        if self.out_remaining >0:
            self.pct_complete = 1-(self.out_remaining / self.archiveFile.uncompressed)
            if self.report_progress:
                print("{:.1f}".format(self.pct_complete * 100), ' percent complete')
                if self.pct_complete>0:
                    cur_time = time.time()
                    seconds_elapsed = cur_time - self.start_time
                    projected_total_time = seconds_elapsed / self.pct_complete
                    projected_remaining_time = projected_total_time - seconds_elapsed
                    print("Estimated seconds remaining: ", projected_remaining_time)
                    #projected_finish_time = time.localtime(cur_time + projected_remaining_time)
                    #proj_finish_str =time.strftime('%Y-%m-%d  %H:%M:%SZ', projected_finish_time)
                    projected_finish_time = time.ctime(cur_time + projected_remaining_time)
                    print("Projected finish time: ",  projected_finish_time)
                    print("Projected total time in seconds: " , projected_total_time)

                    
            tmp = self.decompressor.decompress(self.fp,self.out_remaining) #should look at this to determine the size of chunk it bites off #each chunk was about 4mb, so plenty small
            if len(tmp) >0:
                self.out_remaining -= len(tmp)
                self.crc32 = py7zr.helpers.calculate_crc32(tmp,self.crc32) #update the checksum
                if self.report_fp:
                    print(f"fp is now at {self.fp.tell()}")
                return tmp
        else:
            if self.archiveFile.uncompressed==0:
                print("Following archive file has size 0 bytes, so will not be decompressed")
                raise StopIteration
            else:
                if self.report_progress:
                    print("Archive File Streaming Complete: ", self.archiveFile.filename)
                if self.report_fp:
                    print("Archive file streaming complete, fp is now ", self.fp.tell())
                    print(f"Total change in fp was: {self.fp.tell() - self.starting_fp_position}")
                    print(f"Uncompressed size was: {self.archiveFile.uncompressed}")
                if self.crc32 != self.archiveFile.crc32:
                    raise ChecksumIncorrectError(
                        archive_file = self.archiveFile,
                        calculated_checksum = self.crc32
                    ) 
                raise StopIteration

    def move(self, pct: float, full_size: int):
        print(f"Full size: {full_size}")
        to_move = int(full_size * pct)
        print(f"Old position={self.fp.tell()}")
        self.fp.seek(offset=to_move + self.fp.tell(), whence=0) #means move it relative to the current position
        print(f"New position={self.fp.tell()}")
        #need to do something other than seek - that is throwing an error
        self.out_remaining -= to_move

    def set_start_pct(self, start_pct)-> None:
        print(f"Current fp position is: {self.fp.tell()}")
        print(f"Compressed size is:  {self.archiveFile.compressed}")
        print(f"Out remaining is: {self.out_remaining}")
        

class ChecksumIncorrectError(Exception):
    """Exception raised when the calculated checksum of the decompressed file does not indicate the compressed file's checksum value"""

    def __init__(self, archive_file : py7zr.py7zr.ArchiveFile, calculated_checksum : int):
        self.archive_file = archive_file
        self.calculated_checksum = calculated_checksum
        self.message = "Calculated checksum was incorrect for the following file: " + archive_file.filename
        self.message+="\nCorrect checksum was: " + str(archive_file.crc32)
        self.message+="\nCalculated checksum was: " + str(calculated_checksum)
    
        super().__init__(self.message)


