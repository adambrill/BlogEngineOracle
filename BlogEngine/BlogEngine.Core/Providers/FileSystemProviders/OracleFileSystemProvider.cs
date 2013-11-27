using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Web.Hosting;
using System.IO;
using System.Configuration;
using System.Configuration.Provider;

namespace BlogEngine.Core.Providers
{
    /// <summary>
    /// A class for managing storage on a UNC share.
    /// </summary>
    public partial class OracleFileSystemProvider : BlogFileSystemProvider
    {
        /// <summary>
        /// The conn string name.
        /// </summary>
        public string connStringName;

        /// <summary>
        /// The parm prefix.
        /// </summary>
        private string parmPrefix;

        /// <summary>
        /// The table prefix.
        /// </summary>
        private string tablePrefix;

        /// <summary>
        /// init
        /// </summary>
        /// <param name="name"></param>
        /// <param name="config"></param>
        public override void Initialize(string name, System.Collections.Specialized.NameValueCollection config)
        {
            if (config == null)
            {
                throw new ArgumentNullException("config");
            }

            if (String.IsNullOrEmpty(name))
            {
                name = "DbBlogProvider";
            }

            if (String.IsNullOrEmpty(config["description"]))
            {
                config.Remove("description");
                config.Add("description", "Generic Database Blog Provider");
            }

            base.Initialize(name, config);

            if (config["connectionStringName"] == null)
            {
                // default to BlogEngine
                config["connectionStringName"] = "BlogEngine";
            }

            this.connStringName = config["connectionStringName"];
            config.Remove("connectionStringName");

            if (config["tablePrefix"] == null)
            {
                // default
                config["tablePrefix"] = "be_";
            }

            this.tablePrefix = config["tablePrefix"];
            config.Remove("tablePrefix");

            if (config["parmPrefix"] == null)
            {
                // default
                config["parmPrefix"] = "@";
            }

            this.parmPrefix = config["parmPrefix"];
            config.Remove("parmPrefix");

            // Throw an exception if unrecognized attributes remain
            if (config.Count > 0)
            {
                var attr = config.GetKey(0);
                if (!String.IsNullOrEmpty(attr))
                {
                    throw new ProviderException(string.Format("Unrecognized attribute: {0}", attr));
                }
            }
        }


        private string VirtualPathToUNCPath(string VirtualPath)
        {
            VirtualPath = CleanVirtualPath(VirtualPath);
            VirtualPath = VirtualPath.Replace("/", @"\").Replace(@"\\", @"\").Trim();
            VirtualPath = VirtualPath.StartsWith(@"\") ? VirtualPath : string.Concat(@"\", VirtualPath);
            var storageContainerName = (string.IsNullOrWhiteSpace(Blog.CurrentInstance.StorageContainerName) ? Blog.CurrentInstance.Name : Blog.CurrentInstance.StorageContainerName).Replace(" ", "").Trim();
            var fileContainer = string.Concat("", @"\" ,storageContainerName).Trim();
            if(VirtualPath.ToLower().Contains(fileContainer.ToLower()))
                return VirtualPath;
            return string.Concat(fileContainer,VirtualPath);
        }

        private string CleanVirtualPath(string VirtualPath)
        {
            return VirtualPath.Replace(Blog.CurrentInstance.StorageLocation + Utils.FilesFolder, "").Trim();
        }
        
        /// <summary>
        /// Clears a file system. This will delete all files and folders recursivly.
        /// </summary>
        /// <remarks>
        /// Handle with care... Possibly an internal method?
        /// </remarks>
        public override void ClearFileSystem()
        {
            using (var conn = this.CreateConnection())
            {
                if (conn.HasConnection)
                {
                    using (var cmd = conn.CreateTextCommand(string.Format("DELETE FROM {0}FileStoreFiles", this.tablePrefix)))
                    {
                        cmd.ExecuteNonQuery();
                    }
                    using (var cmd = conn.CreateTextCommand(string.Format("DELETE FROM {0}FileStoreDirectory", this.tablePrefix)))
                    {
                        cmd.ExecuteNonQuery();
                    }
                    using (var cmd = conn.CreateTextCommand(string.Format("DELETE FROM {0}FileStoreFileThumbs", this.tablePrefix)))
                    {
                        cmd.ExecuteNonQuery();
                    }
                }
            }
        }

        /// <summary>
        /// Creates a directory at a specific path
        /// </summary>
        /// <param name="VirtualPath">The virtual path to be created</param>
        /// <returns>the new Directory object created</returns>
        /// <remarks>
        /// Virtual path is the path starting from the /files/ containers
        /// The entity is created against the current blog id
        /// </remarks>
        internal override FileSystem.Directory CreateDirectory(string VirtualPath)
        {
            VirtualPath = CleanVirtualPath(VirtualPath);
            var aPath = VirtualPathToUNCPath(VirtualPath);
            var storageContainerName = (string.IsNullOrWhiteSpace(Blog.CurrentInstance.StorageContainerName) ? Blog.CurrentInstance.Name : Blog.CurrentInstance.StorageContainerName).Replace(" ", "").Trim();
            if (!DirectoryExists(VirtualPath))
            {
                using (var conn = this.CreateConnection())
                {
                    if (conn.HasConnection)
                    {
                        FileSystem.Directory parent = null;
                        var parts = aPath.Split('\\');
                        var partial = "";
                        foreach (var part in parts)
                        {
                            partial += part + "\\";
                            if (!partial.Contains(storageContainerName))
                            {
                                continue;
                            }
                            if (!DirectoryExists(partial))
                            {
                                using (var cmd = conn.CreateTextCommand(string.Format("INSERT INTO {0}FileStoreDirectory (ID, ParentID, BlogID, Name, FullPath, CreateDate, LastAccess, LastModify) VALUES ({1}ID, {1}ParentID, {1}BlogID, {1}Name, {1}FullPath, {1}CreateDate, {1}LastAccess, {1}LastModify)", this.tablePrefix, this.parmPrefix)))
                                {
                                    var parms = cmd.Parameters;
                                    parms.Add(conn.CreateParameter(FormatParamName("ID"), Guid.NewGuid().ToString()));
                                    parms.Add(conn.CreateParameter(FormatParamName("ParentID"), parent == null ? "" : parent.Id.ToString()));
                                    parms.Add(conn.CreateParameter(FormatParamName("BlogID"), Blog.CurrentInstance.Id.ToString()));
                                    parms.Add(conn.CreateParameter(FormatParamName("Name"), part));
                                    parms.Add(conn.CreateParameter(FormatParamName("FullPath"), partial));
                                    parms.Add(conn.CreateParameter(FormatParamName("CreateDate"), DateTime.Now));
                                    parms.Add(conn.CreateParameter(FormatParamName("LastAccess"), DateTime.Now));
                                    parms.Add(conn.CreateParameter(FormatParamName("LastModify"), DateTime.Now));
                                    cmd.ExecuteNonQuery();
                                }
                            }
                            parent = GetDirectory(partial);
                        }
                    }
                }
            }
            return GetDirectory(VirtualPath);
        }

         /// <summary>
        /// Deletes a spefic directory from a virtual path
        /// </summary>
        /// <param name="VirtualPath">The path to delete</param>
        /// <remarks>
        /// Virtual path is the path starting from the /files/ containers
        /// The entity is queried against to current blog id
        /// </remarks>
        public override void DeleteDirectory(string VirtualPath)
        {
            VirtualPath = CleanVirtualPath(VirtualPath);
            if (!this.DirectoryExists(VirtualPath))
                return;
            var aPath = VirtualPathToUNCPath(VirtualPath);
            using (var conn = this.CreateConnection())
            {
                if (conn.HasConnection)
                {
                    using (var cmd = conn.CreateTextCommand(string.Format("DELETE FROM {0}FileStoreDirectory WHERE FullPath LIKE {1}fullpath", this.tablePrefix, this.parmPrefix)))
                    {
                        cmd.Parameters.Add(conn.CreateParameter(FormatParamName("fullpath"), aPath + "%"));
                        cmd.ExecuteNonQuery();
                    }
                }
            }
        }

        /// <summary>
        /// Returns wether or not the specific directory by virtual path exists
        /// </summary>
        /// <param name="VirtualPath">The virtual path to query</param>
        /// <returns>boolean</returns>
        public override bool DirectoryExists(string VirtualPath)
        {
            VirtualPath = CleanVirtualPath(VirtualPath);
            var aPath = VirtualPathToUNCPath(VirtualPath);
            using (var conn = this.CreateConnection())
            {
                if (conn.HasConnection)
                {
                    using (var cmd = conn.CreateTextCommand(string.Format("SELECT FullPath FROM {0}FileStoreDirectory WHERE FullPath = {1}fullpath OR FullPath = {1}fullpath2", this.tablePrefix, this.parmPrefix)))
                    {
                        cmd.Parameters.Add(conn.CreateParameter(FormatParamName("fullpath"), aPath));
                        cmd.Parameters.Add(conn.CreateParameter(FormatParamName("fullpath2"), aPath + "\\"));

                        var row = cmd.ExecuteScalar();

                        return (row != null && !string.IsNullOrEmpty(row.ToString()));
                    }
                }
            }
            return false;
        }

        /// <summary>
        /// gets a directory by the virtual path
        /// </summary>
        /// <param name="VirtualPath">the virtual path</param>
        /// <returns>the directory object or null for no directory found</returns>
        public override FileSystem.Directory GetDirectory(string VirtualPath)
        {
            return GetDirectory(VirtualPath, true);
        }

        /// <summary>
        /// gets a directory by the virtual path
        /// </summary>
        /// <param name="VirtualPath">the virtual path</param>
        /// <param name="CreateNew">unused</param>
        /// <returns>the directory object</returns>
        public override FileSystem.Directory GetDirectory(string VirtualPath, bool CreateNew)
        {
            VirtualPath = CleanVirtualPath(VirtualPath);
            var aPath = VirtualPathToUNCPath(VirtualPath);
            if (!DirectoryExists(VirtualPath))
            {
                CreateDirectory(VirtualPath);
            }
            using (var conn = this.CreateConnection())
            {
                if (conn.HasConnection)
                {
                    using (var cmd = conn.CreateTextCommand(string.Format("SELECT ID, NAME, CREATEDATE, LASTACCESS, LASTMODIFY, FULLPATH FROM {0}FileStoreDirectory WHERE FullPath = {1}fullpath OR FullPath = {1}fullpath2", this.tablePrefix, this.parmPrefix)))
                    {
                        cmd.Parameters.Add(conn.CreateParameter(FormatParamName("fullpath"), aPath));
                        cmd.Parameters.Add(conn.CreateParameter(FormatParamName("fullpath2"), aPath + "\\"));

                        using (var rdr = cmd.ExecuteReader())
                        {
                            rdr.Read();
                            return new FileSystem.Directory
                            {
                                Id = new Guid(rdr.GetString(0)),
                                Name = rdr.IsDBNull(1) ? "" : rdr.GetString(1),
                                DateCreated = rdr.GetDateTime(2),
                                LastAccessTime = rdr.GetDateTime(3),
                                DateModified = rdr.GetDateTime(4),
                                FullPath = rdr.GetString(5),
                                IsRoot = string.IsNullOrWhiteSpace(rdr.GetString(5)),
                            };
                        }
                    }
                }
            }
            return null;
        }

        /// <summary>
        /// gets a directory by a basedirectory and a string array of sub path tree
        /// </summary>
        /// <param name="BaseDirectory">the base directory object</param>
        /// <param name="SubPath">the params of sub path</param>
        /// <returns>the directory found, or null for no directory found</returns>
        public override FileSystem.Directory GetDirectory(FileSystem.Directory BaseDirectory, params string[] SubPath)
        {
            return GetDirectory(string.Join("/", BaseDirectory.FullPath, SubPath), true);
        }

        /// <summary>
        /// gets a directory by a basedirectory and a string array of sub path tree
        /// </summary>
        /// <param name="BaseDirectory">the base directory object</param>
        /// <param name="CreateNew">if set will create the directory structure</param>
        /// <param name="SubPath">the params of sub path</param>
        /// <returns>the directory found, or null for no directory found</returns>
        public override FileSystem.Directory GetDirectory(FileSystem.Directory BaseDirectory, bool CreateNew, params string[] SubPath)
        {
            return GetDirectory(string.Join("/", BaseDirectory.FullPath, SubPath), CreateNew);
        }

         /// <summary>
        /// gets all the directories underneath a base directory. Only searches one level.
        /// </summary>
        /// <param name="BaseDirectory">the base directory</param>
        /// <returns>collection of Directory objects</returns>
        public override IEnumerable<FileSystem.Directory> GetDirectories(FileSystem.Directory BaseDirectory)
        {
            var directories = new List<FileSystem.Directory>();
            using (var conn = this.CreateConnection())
            {
                if (conn.HasConnection)
                {
                    using (var cmd = conn.CreateTextCommand(string.Format("SELECT ID, NAME, CREATEDATE, LASTACCESS, LASTMODIFY, FULLPATH FROM {0}FileStoreDirectory WHERE PARENTID = {1}parentid", this.tablePrefix, this.parmPrefix)))
                    {
                        cmd.Parameters.Add(conn.CreateParameter(FormatParamName("parentid"), BaseDirectory.Id.ToString()));

                        using (var rdr = cmd.ExecuteReader())
                        {
                            while (rdr.Read())
                            {
                                directories.Add(new FileSystem.Directory
                                {
                                    Id = new Guid(rdr.GetString(0)),
                                    Name = rdr.IsDBNull(1) ? "" : rdr.GetString(1),
                                    DateCreated = rdr.GetDateTime(2),
                                    LastAccessTime = rdr.GetDateTime(3),
                                    DateModified = rdr.GetDateTime(4),
                                    FullPath = rdr.GetString(5),
                                    IsRoot = string.IsNullOrWhiteSpace(rdr.GetString(5)),
                                });
                            }
                        }
                    }
                }
            }
            return directories;
        }


        /// <summary>
        /// gets all the files in a directory, only searches one level
        /// </summary>
        /// <param name="BaseDirectory">the base directory</param>
        /// <returns>collection of File objects</returns>
        public override IEnumerable<FileSystem.File> GetFiles(FileSystem.Directory BaseDirectory)
        {
            var files = new List<FileSystem.File>();
            using (var conn = this.CreateConnection())
            {
                if (conn.HasConnection)
                {
                    using (var cmd = conn.CreateTextCommand(string.Format("SELECT FULLPATH, NAME, LASTMODIFY, CREATEDATE, FILEID, LASTACCESS, SIZE_ FROM {0}FileStoreFiles WHERE PARENTDIRECTORYID = {1}parentdirectoryid", this.tablePrefix, this.parmPrefix)))
                    {
                        cmd.Parameters.Add(conn.CreateParameter(FormatParamName("parentdirectoryid"), BaseDirectory.Id.ToString()));

                        using (var rdr = cmd.ExecuteReader())
                        {
                            while (rdr.Read())
                            {
                                files.Add(new FileSystem.File
                                {
                                    FullPath = rdr.GetString(0),
                                    Name = rdr.GetString(1),
                                    DateModified = rdr.GetDateTime(2),
                                    DateCreated = rdr.GetDateTime(3),
                                    Id = rdr.GetString(4),
                                    LastAccessTime = rdr.GetDateTime(5),
                                    ParentDirectory = BaseDirectory,
                                    FilePath = rdr.GetString(0).Replace('\\', '/'),
                                    FileSize = rdr.GetInt32(6),
                                });
                            }
                        }
                    }
                }
            }
            return files;
        }

        /// <summary>
        /// gets a specific file by virtual path
        /// </summary>
        /// <param name="VirtualPath">the virtual path of the file</param>
        /// <returns></returns>
        public override FileSystem.File GetFile(string VirtualPath)
        {
            VirtualPath = CleanVirtualPath(VirtualPath);
            var aPath = VirtualPathToUNCPath(VirtualPath);

            using (var conn = this.CreateConnection())
            {
                if (conn.HasConnection)
                {
                    using (var cmd = conn.CreateTextCommand(string.Format("SELECT FULLPATH, NAME, LASTMODIFY, CREATEDATE, FILEID, LASTACCESS, SIZE_, CONTENTS FROM {0}FileStoreFiles WHERE FULLPATH = {1}fullpath", this.tablePrefix, this.parmPrefix)))
                    {
                        cmd.Parameters.Add(conn.CreateParameter(FormatParamName("fullpath"), aPath));

                        using (var rdr = cmd.ExecuteReader())
                        {
                            rdr.Read();
                            return new FileSystem.File
                            {
                                FullPath = rdr.GetString(0),
                                Name = rdr.GetString(1),
                                DateModified = rdr.GetDateTime(2),
                                DateCreated = rdr.GetDateTime(3),
                                Id = rdr.GetString(4),
                                LastAccessTime = rdr.GetDateTime(5),
                                ParentDirectory = GetDirectory(aPath.Substring(0, aPath.LastIndexOf('/') + 1)),
                                FilePath = rdr.GetString(0).Replace('\\', '/'),
                                FileSize = rdr.GetInt32(6),
                            };
                        }
                    }
                }
            }
            return null;
        }

        /// <summary>
        /// boolean wether a file exists by its virtual path
        /// </summary>
        /// <param name="VirtualPath">the virtual path</param>
        /// <returns>boolean</returns>
        public override bool FileExists(string VirtualPath)
        {
            VirtualPath = CleanVirtualPath(VirtualPath);
            var aPath = VirtualPathToUNCPath(VirtualPath);

            using (var conn = this.CreateConnection())
            {
                if (conn.HasConnection)
                {
                    using (var cmd = conn.CreateTextCommand(string.Format("SELECT FULLPATH FROM {0}FileStoreFiles WHERE FULLPATH = {1}fullpath", this.tablePrefix, this.parmPrefix)))
                    {
                        cmd.Parameters.Add(conn.CreateParameter(FormatParamName("fullpath"), aPath));

                        var row = cmd.ExecuteScalar();

                        return (row != null && !string.IsNullOrEmpty(row.ToString()));
                    }
                }
            }
            return false;
        }

        /// <summary>
        /// deletes a file by virtual path
        /// </summary>
        /// <param name="VirtualPath">virtual path</param>
        public override void DeleteFile(string VirtualPath)
        {
            VirtualPath = CleanVirtualPath(VirtualPath);
            var aPath = VirtualPathToUNCPath(VirtualPath);
            using (var conn = this.CreateConnection())
            {
                if (conn.HasConnection)
                {
                    using (var cmd = conn.CreateTextCommand(string.Format("DELETE FROM {0}FileStoreFiles WHERE FullPath = {1}fullpath", this.tablePrefix, this.parmPrefix)))
                    {
                        cmd.Parameters.Add(conn.CreateParameter(FormatParamName("fullpath"), aPath));
                        cmd.ExecuteNonQuery();
                    }
                }
            }
        }

        /// <summary>
        /// uploads a file to the provider container
        /// </summary>
        /// <param name="FileBinary">file contents as byte array</param>
        /// <param name="FileName">the file name</param>
        /// <param name="BaseDirectory">directory object that is the owner</param>
        /// <returns>the new file object</returns>
        public override FileSystem.File UploadFile(byte[] FileBinary, string FileName, FileSystem.Directory BaseDirectory)
        {
            return UploadFile(FileBinary, FileName, BaseDirectory, false);
        }

        /// <summary>
        /// uploads a file to the provider container
        /// </summary>
        /// <param name="FileBinary">the contents of the file as a byte array</param>
        /// <param name="FileName">the file name</param>
        /// <param name="BaseDirectory">the directory object that is the owner</param>
        /// <param name="Overwrite">boolean wether to overwrite the file if it exists.</param>
        /// <returns>the new file object</returns>
        public override FileSystem.File UploadFile(byte[] FileBinary, string FileName, FileSystem.Directory BaseDirectory, bool Overwrite)
        {
            var virtualPath = string.Format("{0}/{1}", BaseDirectory.FullPath, FileName);
            if (FileExists(virtualPath))
                if (Overwrite)
                    DeleteFile(virtualPath);
                else
                    throw new IOException("File " + virtualPath + " already exists. Unable to upload file.");

            var aPath = VirtualPathToUNCPath(virtualPath);

            using (var conn = this.CreateConnection())
            {
                if (conn.HasConnection)
                {
                    using (var cmd = conn.CreateTextCommand(string.Format("INSERT INTO {0}FileStoreFiles (FILEID, PARENTDIRECTORYID, NAME, FULLPATH, CONTENTS, SIZE_, CREATEDATE, LASTACCESS, LASTMODIFY) VALUES ({1}FileID, {1}ParentDirectoryID, {1}Name, {1}FullPath, {1}Contents, {1}Size_, {1}CreateDate, {1}LastAccess, {1}LastModify)", this.tablePrefix, this.parmPrefix)))
                    {
                        var parms = cmd.Parameters;
                        parms.Add(conn.CreateParameter(FormatParamName("FileID"), Guid.NewGuid().ToString()));
                        parms.Add(conn.CreateParameter(FormatParamName("ParentDirectoryID"), BaseDirectory.Id.ToString()));
                        parms.Add(conn.CreateParameter(FormatParamName("Name"), FileName));
                        parms.Add(conn.CreateParameter(FormatParamName("FullPath"), BaseDirectory.FullPath + FileName));
                        parms.Add(conn.CreateParameter(FormatParamName("Contents"), FileBinary));
                        parms.Add(conn.CreateParameter(FormatParamName("Size_"), FileBinary.Length));
                        parms.Add(conn.CreateParameter(FormatParamName("CreateDate"), DateTime.Now));
                        parms.Add(conn.CreateParameter(FormatParamName("LastAccess"), DateTime.Now));
                        parms.Add(conn.CreateParameter(FormatParamName("LastModify"), DateTime.Now));
                        cmd.ExecuteNonQuery();
                    }
                }
            }

            return GetFile(virtualPath);
        }

        /// <summary>
        /// gets the file contents via Lazy load, however in the DbProvider the Contents are loaded when the initial object is created to cut down on DbReads
        /// </summary>
        /// <param name="BaseFile">the baseFile object to fill</param>
        /// <returns>the original file object</returns>
        internal override FileSystem.File GetFileContents(FileSystem.File BaseFile)
        {
            using (var conn = this.CreateConnection())
            {
                if (conn.HasConnection)
                {
                    using (var cmd = conn.CreateTextCommand(string.Format("SELECT CONTENTS FROM {0}FileStoreFiles WHERE FILEID = {1}fileid", this.tablePrefix, this.parmPrefix)))
                    {
                        cmd.Parameters.Add(conn.CreateParameter(FormatParamName("fileid"), BaseFile.Id.ToString()));

                        var row = cmd.ExecuteScalar();
                        BaseFile.FileContents = (byte[])row;
                    }
                }
            }
            // in case of error, prevent recursive loop
            if (BaseFile.FileContents == null)
            {
                BaseFile.FileContents = new byte[] {};
            }
            return BaseFile;
        }

        /// <summary>
        /// Not implemented. Throws a NotImplementedException.
        /// </summary>
        /// <param name="VirtualPath">unused</param>
        /// <param name="MaximumSize">unused</param>
        /// <returns>Nothing</returns>
        public override FileSystem.Image ImageThumbnail(string VirtualPath, int MaximumSize)
        {
            throw new NotImplementedException();
        }

        /// <summary>
        /// Returns a formatted parameter name to include this DbBlogProvider instance's paramPrefix.
        /// </summary>
        /// <param name="parameterName"></param>
        /// <returns></returns>
        private string FormatParamName(string parameterName)
        {
            return String.Format("{0}{1}", this.parmPrefix, parameterName);
        }

        /// <summary>
        /// Creates a new DbConnectionHelper for this DbBlogProvider instance.
        /// </summary>
        /// <returns></returns>
        private DbConnectionHelper CreateConnection()
        {
            ConnectionStringSettings settings = ConfigurationManager.ConnectionStrings[this.connStringName];
            var helper = new DbConnectionHelper(settings);
            if (helper.Connection.GetType().Name.Contains("Oracle"))
            {
                helper.CreateTextCommand("alter session set NLS_COMP=ANSI").ExecuteNonQuery();
                helper.CreateTextCommand("alter session set NLS_SORT=BINARY_CI").ExecuteNonQuery();
            }
            return helper;
        }
    }
}
