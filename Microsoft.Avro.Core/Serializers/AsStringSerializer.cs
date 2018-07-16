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
namespace Microsoft.Hadoop.Avro.Serializers
{
    using System;
    using System.Buffers.Text;
    using System.Linq.Expressions;
    using System.Reflection;
    using Microsoft.Hadoop.Avro.Schema;

    /// <summary>
    ///     Serializers C# objects using its ToString/Parse methods into an Avro string.
    /// </summary>
    internal sealed class AsStringSerializer : ObjectSerializerBase<PrimitiveTypeSchema>
    {
        /// <summary>
        ///     Initializes a new instance of the <see cref="AsStringSerializer" /> class.
        /// </summary>
        /// <param name="schema">The schema.</param>
        public AsStringSerializer(PrimitiveTypeSchema schema) : base(schema)
        {
        }

        protected override Expression BuildSerializerSafe(Expression encoder, Expression value)
        {
            MethodInfo toString = this.Schema.RuntimeType.GetMethod("ToString", new Type[] { });
            return Expression.Call(
                encoder,
                this.Encode<string>(),
                new Expression[] { Expression.Call(value, toString) });
        }

        protected override Expression BuildDeserializerSafe(Expression decoder)
        {
            var tryParseParameters = new[]
            {
                typeof(ReadOnlySpan<byte>),
                Schema.RuntimeType.MakeByRefType(),
                typeof(int).MakeByRefType(),
                typeof(char)
            };

            MethodInfo tryParse = typeof(Utf8Parser).GetMethod("TryParse", tryParseParameters);

            var p1 = Expression.Call(decoder, Decode("Span", decoder.Type), Expression.Call(decoder, Decode<int>(decoder.Type)));
            var p2 = Expression.Variable(Schema.RuntimeType);
            var p3 = Expression.Variable(typeof(int));
            var p4 = Expression.Constant('\0', typeof(char));

            var parse = Expression.Call(tryParse, p1, p2, p3, p4);

            var parseBlock = Expression.Block(Schema.RuntimeType,
                new ParameterExpression[] { p2, p3 },
                parse, p2);

            return parseBlock;
        }
    }
}
