package org.leo.aws.ddb.repositories;



import org.leo.aws.ddb.annotations.ProjectionType;
import org.leo.aws.ddb.utils.Tuple;

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
    ProjectionType getProjectionType();

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
        Builder projectionType(ProjectionType projectionType);

        /**
         *
         * @return
         */
        GSI build();
    }

}
