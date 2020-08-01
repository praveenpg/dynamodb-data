package org.leo.aws.ddb.repositories;



import org.leo.aws.ddb.annotations.GlobalSecondaryIndex;
import org.leo.aws.ddb.utils.model.Tuple;

import java.lang.reflect.Field;

interface GSI {
    String getName();


    /**
     *
     * @return
     */
    Tuple<String, Field> getHashKeyTuple();

    /**
     *
     * @return
     */
    Tuple<String, Field> getRangeKeyTuple();

    /**
     *
     * @return
     */
    GlobalSecondaryIndex.ProjectionType getProjectionType();

    /**
     *
     * @param indexName
     * @return
     */
    static Builder builder(final String indexName) {
        return GlobalSecondaryIndexImpl.builder(indexName);
    }

    @SuppressWarnings("UnusedReturnValue")
    interface Builder {
        /**
         *
         * @param hashKeyTuple
         * @return
         */
        Builder hashKeyTuple(Tuple<String, Field> hashKeyTuple);

        /**
         *
         * @param rangeKeyTuple
         * @return
         */
        Builder rangeKeyTuple(Tuple<String, Field> rangeKeyTuple);

        /**
         *
         * @param projectionType
         * @return
         */
        Builder projectionType(GlobalSecondaryIndex.ProjectionType projectionType);

        /**
         *
         * @return
         */
        GSI build();
    }

}
