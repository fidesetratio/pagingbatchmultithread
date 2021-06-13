package pagingbatch;

import org.springframework.batch.core.ChunkListener;
import org.springframework.batch.core.scope.context.ChunkContext;

public class CustomChunkListener implements ChunkListener {
    @Override
    public void afterChunk(ChunkContext chunkContext) {
        System.out.println("Chunk processing ended " + chunkContext);
    }

    @Override
    public void afterChunkError(ChunkContext chunkContext) {

    }

    @Override
    public void beforeChunk(ChunkContext chunkContext) {
        System.out.println("Chunk processing started " + chunkContext);
    }
}