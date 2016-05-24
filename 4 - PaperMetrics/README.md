#### Indegree-Outdegree

The indegree outdegree is calculated for each paper by scanning the entire `CitaionNetwork` table int the `PaperMetricIndegreeOutdegree.java` file.

It comes to note however that each paper in the `PaperAbout` table may not citations in the `CitationNetwork` table.

Thus we use `PaperMetricIndegreeOutdegreePatch.java` to make such indegrees & outdegrees `0` instead of `null`.


#### Median Indegree

To compute the median, we have merely sorted the indegrees and written them to a file and then extracted the exact middle row of file if number of rows are even or the average of the middle rows if the number if odd.