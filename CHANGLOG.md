* Remove SqlgTransaction.batchCommit()
    It is no longer usefull as the embedded topology change forced sqlg to auto flush the batch before any query.
    This makes the result of batchCommit() incorrect as it exclues the auto flushed elements.