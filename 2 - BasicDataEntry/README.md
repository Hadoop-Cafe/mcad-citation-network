#### DataEntryComputerScienceFieldsTable.java

This file inputs the output of phase (1) into an HBase table called `ComputerScienceFieldsTable`.


#### DataEntryPaperAboutTableCommunities.java

This file takes input from `PaperKeywords.txt` and stores only computer science domain papers into `PaperAbout` table.


#### FilterCitations.java

This file takes input from `Citations.txt` and stores only computer science domain citations into `CitationNetwork` table.