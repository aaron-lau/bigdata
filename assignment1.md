Tested on local machine and student environment (ubuntu1604-006)

# q1
The PairsPMI consist of 3 map reduce jobs. The first map reduce job takes input from the input
file and counts the number of lines which is necessary for PMI calculation. The line count is then written to a file
in a lineCount directory. The second map reduce job takes input from the input file and
obtains occurrences and cooccurrence of words for each event.
These numbers are also required for PMI calculations and are placed in an
intermediate directory. The third map reduce job takes input from the second map reduce jobs.
The third mapper will simply convert <PairsOfStrings, FloatWritable> key value pairs outputted from the second mapper to
<PairsOfStrings, PairOfFloatInt>. The float is initialized to be 0.0 and will be used to calculate
PMI in the reduce step. The Int is the co-occurrence count that was already calculated from
the second mapper. The PairOfStrings is the two words we are calculating the PMI and co-occurrence
for. The reducer will read the side data stored in lineCount and intermediate directories
in the reducer setup. After parsing the side data the reduce step simply performs the PMI calculation and will then write a
<PairsOfStrings, PairOfFloatInt> to the context.

The StripesPMI is quite similar to the PairsPMI. Again there are three map reduce jobs. The first two map reduce jobs
do exactly the same thing but the second map reduce job will only output occurrences and not co-occurrences.
The third map reduce job will take input from the input file. The mapper will emit a key value
pair where the key is a word (e.g., A) and the value is a map, where each of the keys is a
co-occurring word (e.g., B), and the value is an integer 1 corresponding to the co-occurrence
count. The combiner will then combine all the key/value pairs where the map in the value have
the same key. The setup in the combiner is similar to PairsPMI where it parsers and stores the
side data outputted in the first two map reduce jobs. Using the co-occurrence obtained in the
third map reduce and the line count and occurrence count obtained from the side data, the reduce
job will calculate the PMI and output a <Text, Text> key/value pair. The key is a word (e.g., A)
and the value is a map where the key is a co-occurring work (e.g., B) and the value is a pair
of PMI and co-occurrence value (e.g., 'A	B: (PMI, CO-OCCURRENCE)'). The output is in the form
of Text because I was unable to find a hashmap writable with a string writable key and a pair
writable value.

# q2
Pairs: 65.211 seconds
Stripes: 25.84 seconds
Ran in my local environment

# q3
Disabled combiners:
Pairs: 67.884 seconds
Stripes: 20.583 seconds (Which is odd and makes me question if my reducer is actually slowly down my program...)
Ran in my local environment

# q4
Using threshold 10, 38506 distinct PMI pairs were created

# q5
The following is the pair of words with the highest PMI
(haven, milford)	(3.6201773, 11)
This means that 'haven' and 'milford' are likely to occur together on the same line

The following is the pair of words with the lowest PMI
(when, of)	(-2.634309, 16)
This means that 'when' and 'of' are not likely to occur together on the same line
compared to if they were independent.

# q6
The following are the three pairs with the highest PMI with the word 'tears'
Their respective PMI is the first number in the second parenthesis
Their respective cooccurrence is the second number in the second parenthesis
(tears, eyes)	(-0.038953, 23)
(tears, heart)	(-0.5535282, 10)
(tears, her)	(-0.7268072, 24)

The following are the three pairs with the highest PMI with the word 'death'
Their respective PMI is the first number in the second parenthesis
Their respective cooccurrence is the second number in the second parenthesis
(death, father's)	(-0.08386797, 21)
(death, life)	(-0.4659854, 31)
(death, after)	(-0.64235836, 10)

# q7
The following the the fair pairs with the highest PMI with the word 'hockey'
Their respective PMI is the first number in the second parenthesis
Their respective cooccurrence is the second number in the second parenthesis
hockey	winger: (2.3863757, 185)
hockey	defenceman: (2.2691333, 108)
hockey	ice: (2.1059084, 1630)
hockey	goaltender: (2.0803165, 136)
hockey	nhl: (1.9864639, 940)

# q8
The following the the fair pairs with the highest PMI with the word 'hockey'
Their respective PMI is the first number in the second parenthesis
Their respective cooccurrence is the second number in the second parenthesis
data	storage: (1.9796829, 100)
data	stored: (1.7868549, 65)
data	database: (1.7758231, 73)
data	disk: (1.7383235, 59)
data	processing: (1.6476576, 57)
