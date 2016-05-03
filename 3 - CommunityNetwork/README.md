#### CreateCommunityGraph.java

This file creates a community graph using the tables `CitaionNetwork` and `PaperAbout`

For each row in `CitaionNetwork` get the fromPaperId and toPaperId.

For each paperId get the research communities that the paper is a part of i.e. fromPaperCommunities & toPaperCommunities.

Add an edge from each community in fromPaperCommunities to each community in toPaperCommunities.