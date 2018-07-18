// Copyright (c) Microsoft Corporation
// All rights reserved.
// 
// Licensed under the Apache License, Version 2.0 (the "License"); you may not
// use this file except in compliance with the License.  You may obtain a copy
// of the License at http://www.apache.org/licenses/LICENSE-2.0
// 
// THIS CODE IS PROVIDED *AS IS* BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, EITHER EXPRESS OR IMPLIED, INCLUDING WITHOUT LIMITATION ANY IMPLIED
// WARRANTIES OR CONDITIONS OF TITLE, FITNESS FOR A PARTICULAR PURPOSE,
// MERCHANTABLITY OR NON-INFRINGEMENT.
// 
// See the Apache Version 2.0 License for specific language governing
// permissions and limitations under the License.

using System;

namespace Microsoft.Hadoop.Avro
{
    /// <summary>
    /// Extension methods for <see cref="IAvroSerializer{T}"/>.
    /// </summary>
    public static class AvroSerializerExtensions
    {
        /// <summary>
        /// Deserializes an object from the specified <paramref name="span"/>.
        /// </summary>
        /// <param name="serializer">The serializer.</param>
        /// <param name="span">The span.</param>
        /// <returns>The deserialized object.</returns>
        public static T Deserialize<T>(this IAvroSerializer<T> serializer, ReadOnlySpan<byte> span)
        {
            return serializer.Deserialize(span, out int bytesConsumed);
        }
    }
}
