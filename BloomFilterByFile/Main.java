
import com.newbloom.common.hash.BloomFilter;
import com.newbloom.common.hash.Funnels;

import java.io.File;

public class Main {

    public static void main(String[] args) {

        String filename="BLOOM_BITARRAY_FILE";

        //delete file
        new File(filename).delete();

         long expectedInsertions=1024*2;
        BloomFilter<String> bloomFilter =
                BloomFilter.createByFile(Funnels.unencodedCharsFunnel(),
                        filename,
                        expectedInsertions);


        String value = "test str";
        if (!bloomFilter.put(value)) {
            System.out.println("put false");
        }
        for (long i = 0; i <expectedInsertions ; i++) {
            if(i==expectedInsertions/2)
                continue;
            if (!bloomFilter.put(value+i)) {
                System.out.println("put false");
            }
        }

        System.out.println("mightContain :" + bloomFilter.mightContain(value));
        System.out.println("mightContain :" + bloomFilter.mightContain(value+expectedInsertions/2));
        System.out.println("mightContain :" + bloomFilter.mightContain(value+(expectedInsertions-2)));

    }
}
