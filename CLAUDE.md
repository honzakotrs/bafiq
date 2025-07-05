# Project definition

Bafiq (BAM Flag Index Query) is a bioinformatics rust tool used to efficiently
index the sorted BAM file sequences' flags. It's inteded use-case is repeated
sequence of different flag queries (combinations, exclusions) producing
immediate counts. The index also carries pointers to the original BAM file
blocks containing the sequences matching given flag query making it potentially
faster to extract sequences based on flag combinations.

## Key aspects

- The index uses several layers of semantic compression
- Different strategies are available to choose from while building the index
ranging from simple single-core sequential to fully paralellized chunking
approach
- Performance is measured against baselined samtools view -c -f 0x4 with the goal
of being faster leveraging rust language features and ecosystem

## Stack

- Using justfile to shorten the key commands
- Criterion to drive the benchmarks (index building strategies)

## Principles

- Use DRY (do not repeat yourslef)
- Use KISS (keep it simple stupid)
- never duplicate unless explicitly asked (temporary tests etc.)
- do not write tests unless asked as the code might evolve further first
- before removing files make sure with the user that changes are commited
