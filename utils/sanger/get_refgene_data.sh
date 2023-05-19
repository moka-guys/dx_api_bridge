#!/bin/sh
ORG=$1
REFGENE=$2
UCSCENSEMBL=${REFGENE}'.chromosomes'

# 1 r.bin, 2 r.name, 3 r.chrom, 4 r.strand, 5 r.txStart, 6 r.txEnd, 7 r.cdsStart, 8 r.cdsEnd, 9 r.exonCount,
# 10 r.exonStarts, 11 r.exonEnds, 12 r.score, 13 r.name2, 14 r.cdsStartStat, 15 r.cdsEndStat, 16 r.exonFrames

echo "Getting refGene table..."
mysql --user=genome --host=genome-mysql.cse.ucsc.edu -A -N -D $ORG -P 3306 \
 -e 'SELECT r.bin,CONCAT(r.name,".",i.version),r.chrom,r.strand,r.txStart,r.txEnd,
r.cdsStart,r.cdsEnd,r.exonCount,r.exonStarts,r.exonEnds,r.score,r.name2,
r.cdsStartStat,r.cdsEndStat,r.exonFrames from refGene as r, hgFixed.gbCdnaInfo as i where r.name=i.acc ORDER BY r.bin;' > ${REFGENE}.${ORG}

# get hg/GRCh conversion
echo "Getting UCSC->Ensembl chromosome table..."
mysql --user=genome --host=genome-mysql.cse.ucsc.edu -A -N -D $ORG -P 3306 \
 -e "select c.ucsc,c.ensembl,i.size from chromInfo as i, ucscToEnsembl as c where c.ucsc = i.chrom;" > ${UCSCENSEMBL}

## convert to UCSC chromosome names
echo "Converting refGene table to GRCh chromosome names..."
awk 'BEGIN { OFS = "\t" };
{
  if (NR == FNR) {
    chrom[$1] = $2
  }
  else {
    $3=chrom[$3]
    print $0
  }
}
' ${UCSCENSEMBL} ${REFGENE}.${ORG} > ${REFGENE}

## get reference sequence
echo "Getting reference sequence..."
wget -c ftp://ftp.1000genomes.ebi.ac.uk/vol1/ftp/technical/reference/phase2_reference_assembly_sequence/hs37d5ss.fa.gz
