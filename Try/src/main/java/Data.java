
public class Data {

    // read the vcf file and store each row in database, in Data table
    // parser
    // VCF Reader existing

    String chrom;
    String pos;   // int pos
    String id;
    String ref;
    String alt;
    String qual; // Integer ?
    String filter;
    String info;

    // 8 fields
    // todo if is pos/ parsetoint

    public Data(String chrom, String pos, String id, String ref, String alt, String qual, String filter, String info) {

        this.chrom=chrom;
        this.pos=pos;
        this.id=id;
        this.ref=ref;
        this.alt=alt;
        this.qual=qual;
        this.filter=filter;
        this.info=info;

    }

    // Setters and getters

    public void setId(String id)
    {
        this.id = id;
    }

    // TODO BIF BOF
    public Integer getId () {
        return 1;
    }

    public void setChrom(String chrom)
    {
        this.chrom = chrom;
    }

    public String getChrom()
    {
        return chrom;
    }

    public void setPos(String Pos)
    {
        this.pos = pos;
    }

    public String getPos()
    {
        return pos;
    }

    public void setRef(String ref)
    {
        this.ref = ref;
    }

    public String getRef()
    {
        return ref;
    }

    public void setAlt(String chrom)
    {
        this.alt = alt;
    }

    public String getAlt()
    {
        return alt;
    }

    public void setQual(String qual)
    {
        this.qual = qual;
    }

    public String getQual()
    {
        return qual;
    }


    // Lire le fichier une fois pour toute et inserer au fur et a mesure dans la table - Boucle
    // Connection to database blablabal
    //INSERT INTO Data (Chrom,Pos, Id,Ref,Alt,Qual,Filter,Info)


}