package bitcoin;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public class BlockReader {

    private InputStream  input;

    public BlockReader(InputStream input) {
        this.input=input;
    }

    public Block next() {
        try {
            Block result = new Block();

            byte[] magicBytes = new byte[4];
            int magicBytesLen = input.read(magicBytes);
            if (magicBytesLen != 4) {
                return null;
            }

            byte[] blockLenBytes=new byte[4];
            int blockBytesLen=input.read(blockLenBytes);
            if (blockBytesLen != 4) {
                return null;
            }

            ByteBuffer buffLen=ByteBuffer.wrap(blockLenBytes);
            buffLen.order(ByteOrder.LITTLE_ENDIAN);
            long blockLen=Integer.toUnsignedLong(buffLen.getInt());
            if (blockLen<=0 || blockLen> 8*1024*1024) {
                return null;
            }

            byte[] bytesBlock=new byte[(int)blockLen];
            int total=0;
            int rbyte;
            while ((rbyte=input.read(bytesBlock,total,(int)blockLen-total))>-1) {
                total+=rbyte;
                if (total>=blockLen) {
                    break;
                }
            }
            ByteBuffer inputBlock=ByteBuffer.wrap(bytesBlock);
            inputBlock.order(ByteOrder.LITTLE_ENDIAN);

            processHeader(inputBlock,result);

            long transCounter=Utils.readVarInt(inputBlock);

            processTransaxtions(transCounter,result,inputBlock);

            return result;

        }catch(IOException e) {

        }
        return null;
    }

    private void processHeader(ByteBuffer inputBlock,Block block) {

        block.setVersion(inputBlock.getInt());

        byte[] prevBlock=new byte[32];
        inputBlock.get(prevBlock,0,32);
        block.setPrevHash(prevBlock);

        byte[] rootBlock=new byte[32];
        inputBlock.get(rootBlock,0,32);
        block.setRootHash(rootBlock);

        block.setTime(inputBlock.getInt());
        block.setnBits(inputBlock.getInt());
        block.setNonce(inputBlock.getInt());

    }

    private void processTransaxtions(long num,Block block,ByteBuffer inputBlock) {

        byte[] arr=inputBlock.array();

        for(int i=0;i<num;i++) {
            BitTransaction transaction = new BitTransaction();

            int startOffs=inputBlock.position();

            int version=inputBlock.getInt();
            long inCounter=Utils.readVarInt(inputBlock);
            boolean isSegwit=false;
            int layout=1;
            if (inCounter==0) {
                inputBlock.mark();
                byte segwit=inputBlock.get();
                if (segwit!=0) {
                    layout=0;
                    isSegwit=true;
                    inCounter=Utils.readVarInt(inputBlock);
                } else {
                    inputBlock.reset();
                }
            }

            boolean wasNull=processInputs(inputBlock,inCounter,transaction);

            long outCounter=Utils.readVarInt(inputBlock);

            processOutputs(inputBlock,outCounter,transaction);

            if (isSegwit) {
                for(int j=0;j<inCounter;j++) {
                    long witnCounter=Utils.readVarInt(inputBlock);
                    for(int k=0;k<witnCounter;k++) {
                        long scriptSize=Utils.readVarInt(inputBlock);
                        byte[] scriptBytes=new byte[(int)scriptSize];
                        inputBlock.get(scriptBytes,0,(int)scriptSize);
                    }
                }
            }

            int lockTime=inputBlock.getInt();

            lockTime=block.getTime();

            transaction.setLockTime(lockTime);

            int endOffs=inputBlock.position();
            byte[] buf = new byte[endOffs-startOffs];

            System.arraycopy(arr, startOffs, buf, 0, endOffs-startOffs);
            String txid = Utils.encodeBase16(Utils.reverseBytes(Utils.hashTwice(buf)),false);
            transaction.setId(txid);

            block.addTransaction(transaction);
        }
    }

    private static String zeroHash = Utils.encodeBase16(new byte[32],false);

    private boolean processInputs(ByteBuffer inputBlock, long inCounter, BitTransaction transaction) {
        boolean r=false;
        for(int i=0;i<inCounter;i++) {
            byte[] prevTransactionHash=new byte[32];
            inputBlock.get(prevTransactionHash,0,32);
            long prevTxOutIdx=Integer.toUnsignedLong(inputBlock.getInt());

            long scriptSize=Utils.readVarInt(inputBlock);
            byte[] scriptBytes=new byte[(int)scriptSize];
            inputBlock.get(scriptBytes,0,(int)scriptSize);
            long seqNo=Integer.toUnsignedLong(inputBlock.getInt());
            if (ScriptParser.getToAddress(scriptBytes)==null) {
                r=true;
                ScriptParser.getToAddress(scriptBytes);
            }
            boolean isCoinBase=false;
            String id=Utils.encodeBase16(Utils.reverseBytes(prevTransactionHash),false);
            if  ((prevTxOutIdx & 0xFFFFFFFFL) == 0xFFFFFFFFL && id.equals(zeroHash))
                isCoinBase=true;
            transaction.addIn(new BitTransaction.Tuple(ScriptParser.getToAddress(scriptBytes),
                    id,(int)prevTxOutIdx,isCoinBase));

        }
        return r;
    }

    private void processOutputs(ByteBuffer inputBlock, long outCounter, BitTransaction transaction) {
        for(int i=0;i<outCounter;i++) {
            long outValue=inputBlock.getLong();
            long scriptSize=Utils.readVarInt(inputBlock);
            byte[] scriptBytes=new byte[(int)scriptSize];
            inputBlock.get(scriptBytes,0,(int)scriptSize);
            transaction.addOut(new BitTransaction.Tuple(ScriptParser.getToAddress(scriptBytes),outValue));
        }
    }
}
