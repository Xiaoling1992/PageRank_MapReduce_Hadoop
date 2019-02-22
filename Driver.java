
public class Driver {

    public static void main(String[] args) throws Exception {

        UnitMultiplication multiplication = new UnitMultiplication();
        UnitSum sum = new UnitSum();

        //args0: dir of transition.txt
        //args1: dir of PageRank.txt
        //args2: dir of unitMultiplication result
        //args3: times of convergence
        //args4: the beta
        String transitionMatrix = args[0];
        String prMatrix = args[1];
        String unitState = args[2];

        int count = Integer.parseInt(args[3]);
        String beta =args[4];  //We keep the arguments to be Strings so that we can use String[] to pass arguments.
        for(int i=0;  i<count;  i++) {
            String[] args1 = {transitionMatrix, prMatrix+i, unitState+i};
            multiplication.main(args1);

            String[] args2 = {prMatrix+i, unitState + i, prMatrix+(i+1), beta};
            sum.main(args2);
        }
    }
}
