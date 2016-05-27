import os

def write_csv(metric, start_year, end_year_plus_one, interval, metric_dict):
	all_comm = ['AI', 'ALGO', 'ARC', 'BIO', 'CV', 'DB', 'DIST', 'DM', 'GRP', 'HCI', 'IR', 'ML', 'MUL', 'NLP', 'NW', 'OS', 'PL', 'RT', 'SC', 'SE', 'SEC', 'SIM', 'WWW']
	csv = open(metric + '_' + str(interval) + '.csv', 'w')
	csv.write('start_year,end_year+1,' + ','.join(all_comm) + '\n')
	for year in range(start_year, end_year_plus_one, interval):
		csv.write(str(year) + ',' + str(year + interval) + ',')
		for comm in all_comm:
			if (str(year), interval, comm, metric) in metric_dict.keys():
				csv.write(metric_dict[(str(year), interval, comm, metric)] + ',')
			else:
				csv.write('0,')
		csv.write('\n')

# os.system('echo \'scan "TemporalTidings"\' | hbase shell > tmp1')
# os.system('tail -n +7 tmp1 > tmp2')
# os.system('head -n -2 tmp2 > TemporalTidings.txt')
# os.system('rm tmp1 tmp2')

org_file = open('TemporalTidings.txt')
lines = org_file.readlines()

# print ((lines[0]))
lines = [line.strip() for line in lines]
# print (lines[0])
lines = [line.split(',')[0] + line.split(',')[2] for line in lines]
# print (lines[0])
lines = [line.split(' ')[0][0:9] + ' ' + line.split(' ')[0][10:] + line[line.find(' '):] for line in lines]
# print (lines[0])

metric_dict = {}

for line in lines:
	split_line = line.split(' ')
	start_year = split_line[0][0:4]
	end_year = split_line[0][5:9]
	interval = int(end_year) - int(start_year)
	comm = split_line[1]
	metric = split_line[2].split(':')[1]
	value = split_line[3].split('=')[1]

	# if metric == 'count' and interval == 10:
	# 	print (line)
	# 	print ((start_year, interval, comm, metric), value)

	metric_dict[(start_year, interval, comm, metric)] = value

metrics = ['inwardness', 'cut', 'expansion', 'count']
start_year = 1960
end_year_plus_one = 2020
interval = 1

for metric in metrics:
	write_csv(metric, start_year, end_year_plus_one, interval, metric_dict)
	pass
