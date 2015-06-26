using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Net.Http;
using System.Text;
using ICSharpCode.SharpZipLib.BZip2;
using ICSharpCode.SharpZipLib.Tar;

namespace XmcdParser
{
	class Program
	{
		private const int BatchSize = 24;
		static void Main()
		{
		    var client = new HttpClient();
            var uri = new Uri("http://localhost:1666/api/albums");

            var sp = Stopwatch.StartNew();
            foreach (var disk in ParseDisks(@"D:\github\freedb-complete-20150601.tar.bz2"))
            {
                try
                {
                    var res = client.PostAsJsonAsync(uri, disk).Result;
                    Console.WriteLine(res.ToString());
                }
                catch (Exception e)
                {
                    Console.WriteLine(e.ToString());
                }
				Console.WriteLine("Done in {0}", sp.Elapsed);
			}

            Console.ReadLine();
		}

		private static IEnumerable<Disk> ParseDisks(string filePath)
		{
			int i = 0;
			var parser = new Parser();
			var buffer = new byte[1024*1024];// more than big enough for all files

            using (var bz2 = new BZip2InputStream(File.Open(filePath, FileMode.Open)))
			using (var tar = new TarInputStream(bz2))
			{
				TarEntry entry;
				while((entry=tar.GetNextEntry()) != null)
				{
					if(entry.Size == 0 || entry.Name == "README" || entry.Name == "COPYING")
						continue;
					
                    var readSoFar = 0;
					while(true)
					{
						var read = tar.Read(buffer, readSoFar, ((int) entry.Size) - readSoFar);
						if (read == 0)
							break;

						readSoFar += read;
					}
					// we do it in this fashion to have the stream reader detect the BOM / unicode / other stuff
					// so we can read the values properly
					var fileText = new StreamReader(new MemoryStream(buffer,0, readSoFar), Encoding.UTF8).ReadToEnd();
				    Disk disk = null;
					try
					{
						disk = parser.Parse(fileText);
					}
					catch (Exception e)
					{
						Console.WriteLine();
						Console.WriteLine(entry.Name);
						Console.WriteLine(e);
                    }
                    if(disk != null)
                    {
                        yield return disk;
                    }
				}
                yield break;
			}
		}
	}
}