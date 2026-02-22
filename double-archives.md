# Double archived NZBs

We can handle streaming of 7z rar4 and rar5 archives well provided they do not actually apply any compression.
We do this by using the kotlin-compression-tools library to determine which byte ranges hold the content we
want to stream.

Some nzbs have archives that are "double" though. For instance a 7zip archive that contains another rar4 archive.
I want to know how complicated it would be to extract metadata from the nested archive find the SplitParts that we can
use to stream file inside the nested archive. There is an example of such an archive located in
/home/william/IdeaProjects/nzb-streamer-utils/lmdoublerar