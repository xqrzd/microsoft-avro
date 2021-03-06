﻿// Copyright (c) Microsoft Corporation
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

namespace Microsoft.Hadoop.Avro.Tests
{
    using System;
    using System.Collections.Concurrent;
    using System.Collections.Generic;
    using System.Collections.ObjectModel;
    using System.Diagnostics.CodeAnalysis;
    using System.IO;
    using System.Linq;
    using System.Linq.Expressions;
    using System.Runtime.Serialization;
    using System.Text;
    using System.Threading;
    using Microsoft.Hadoop.Avro;
    using Microsoft.Hadoop.Avro.Schema;
    using Microsoft.Hadoop.Avro.Serializers;
    using Microsoft.Hadoop.Avro.Tests.Classes;
    using Xunit;

    [Trait("Category","AvroSerializer")]
    public sealed class AvroSerializerTests
    {
        #region Different type tests

        [Fact]
        public void Serializer_CreationWithType()
        {
            var serializer = AvroSerializer.Create<int>();
            Assert.NotNull(serializer);
        }

        [Fact]
        public void Serializer_SerializeInt()
        {
            RoundTripSerializationWithCheck(13);
        }

        [Fact]
        public void Serializer_SerializeEnum()
        {
            RoundTripSerializationWithCheck(Utilities.RandomEnumeration.Value3);
        }

        [Fact]
        public void Serializer_SerializeBitEnum()
        {
            RoundTripSerializationWithCheck(FlagsEnumClass.Create());
        }

        [Fact]
        public void Serializer_SerializeClassWithTwoFieldsOfSameType()
        {
            RoundTripSerializationWithCheck(TwoFieldsOfTheSameTypeClass.Create());
        }

        [Fact]
        public void Serializer_SerializeClassWithIntField()
        {
            RoundTripSerializationWithCheck(ClassOfInt.Create(true));
        }

        [Fact]
        public void Serializer_SerializeClassWithFields()
        {
            RoundTripSerializationWithCheck(ClassWithFields.Create());
        }

        [Fact]
        public void Serializer_SerializeNestedClass()
        {
            RoundTripSerializationWithCheck(NestedClass.Create(true));
        }

        [Fact]
        public void Serializer_SerializeRecursiveClass()
        {
            RoundTripSerializationWithCheck(Recursive.Create());
        }

        [Fact]
        public void Serializer_SerializeSimpleFlatClass()
        {
            RoundTripSerializationWithCheck(SimpleFlatClass.Create());
        }

        [Fact]
        public void Serializer_SerializeComplexNestedClass()
        {
            RoundTripSerializationWithCheck(ComplexNestedClass.Create());
        }

        [Fact]
        public void Serializer_SerializeListOfComplexNestedClass()
        {
            RoundTripSerializationWithCheck(new List<ComplexNestedClass>
            {
                ComplexNestedClass.Create(),
                ComplexNestedClass.Create()
            });
        }

        [Fact]
        public void Serializer_SerializeGuidClass()
        {
            RoundTripSerializationWithCheck(ClassOfGuid.Create(true));
        }

        [Fact]
        public void Serializer_SerializerEmptyStruct()
        {
            var expected = new EmptyStruct();
            RoundTripSerializationWithCheck(
                expected,
                actual => { });
        }

        [Fact]
        public void Serializer_SerializerComplexStructWithNull()
        {
            var expected = new ComplexStruct();
            RoundTripSerializationWithCheck(
                expected);
        }

        [Fact]
        public void Serializer_SerializerComplexStructWithValues()
        {
            var expected = new ComplexStruct(new List<int> { 1, 2, 3 });
            RoundTripSerializationWithCheck(expected);
        }

        [Fact]
        public void Serializer_SerializeClassWithUri()
        {
            RoundTripSerializationWithCheck(ContainingUrlClass.Create());
        }

        [Fact]
        public void Serializer_SerializeAnonymousClass()
        {
            var testObject = new { IntField = 13, DoubleField = 13.0 };
            RoundTripSerializationWithCheck(testObject, new AvroSerializerSettings { Resolver = new AvroPublicMemberContractResolver() });
        }

        [Fact]
        public void Serializer_SerializeGenericListWithAnonymousTypeParameter()
        {
            var expected = new[]
            {
                new { Name = Utilities.GetRandom<string>(false), Value = Utilities.GetRandom<int>(false) },
                new { Name = Utilities.GetRandom<string>(false), Value = Utilities.GetRandom<int>(false) },
                new { Name = Utilities.GetRandom<string>(false), Value = Utilities.GetRandom<int>(false) }
            };
            RoundTripSerializationWithCheck(expected, actual => Assert.True(expected.SequenceEqual(actual)), new AvroSerializerSettings { Resolver = new AvroPublicMemberContractResolver() });
        }

        [Fact]
        public void Serializer_SerializeGenericClassWithAnonymousTypeParameter()
        {
            var expected = GenericClassBuilder.Build(new { Name = Utilities.GetRandom<string>(false), Value = Utilities.GetRandom<int>(false) });
            RoundTripSerializationWithCheck(expected, new AvroSerializerSettings { Resolver = new AvroPublicMemberContractResolver() });
        }

        [Fact]
        public void Serializer_SerializeArrayClass()
        {
            RoundTripSerializationWithCheck(ArrayClass.Create());
        }

        [Fact]
        public void Serializer_SerializeJaggedArrayClass()
        {
            RoundTripSerializationWithCheck(JaggedArrayClass.Create());
        }

        [Fact]
        public void Serializer_SerializeMultidimensionalArrayClass()
        {
            RoundTripSerializationWithCheck(MultidimArrayClass.Create());
        }

        [Fact]
        public void Serializer_SerializeEnumClass()
        {
            RoundTripSerializationWithCheck(ClassOfEnum.Create(true));
        }

        [Fact]
        public void Serializer_SerializeFixed()
        {
            RoundTripSerializationWithCheck(AvroFixedClass.Create(7));
        }

        [Fact]
        public void Serializer_SerializeFixedWithWrongSize()
        {
            Assert.Throws<SerializationException>(() =>
                {
                    var obj = AvroFixedClass.Create(5);
                    RoundTripSerializationWithCheck(obj);
                }
            );
        }
        #endregion

        #region List tests
        [Fact]
        public void Serializer_SerializeListClass()
        {
            RoundTripSerializationWithCheck(ClassOfListOfGuid.Create(true));
        }

        [Fact]
        public void Serializer_SerializeIList()
        {
            var knownTypes = new[] { typeof(List<Guid>), typeof(List<int>) };
            RoundTripSerializationWithCheck(IListClass.Create(), new AvroSerializerSettings { KnownTypes = knownTypes });
        }

        [Fact]
        public void Serializer_SerializeIListWithArray()
        {
            var knownTypes = new[] { typeof(Guid[]), typeof(List<int>) };
            RoundTripSerializationWithCheck(IListClass.CreateWithArray(), new AvroSerializerSettings { KnownTypes = knownTypes });
        }

        [Fact]
        public void Serializer_SerializeIListWithCollection()
        {
            var knownTypes = new[] { typeof(Collection<Guid>), typeof(Collection<int>) };
            RoundTripSerializationWithCheck(IListClass.CreateWithCollection(), new AvroSerializerSettings { KnownTypes = knownTypes });
        }

        [Fact]
        public void Serializer_SerializeInheritedList()
        {
            var testObject = new ListInheritedClass<int> { 1, 2, 3 };
            RoundTripSerializationWithCheck(testObject);
        }

        [Fact]
        public void Serializer_SerializeListOfLists()
        {
            var expected = new List<List<ClassOfInt>>
            {
                new List<ClassOfInt> { ClassOfInt.Create(true), ClassOfInt.Create(true) },
                new List<ClassOfInt> { ClassOfInt.Create(true), ClassOfInt.Create(true) }
            };

            RoundTripSerializationWithCheck(
                expected,
                actual =>
                {
                    Assert.Equal(expected.Count, actual.Count);
                    expected.Select(
                        (e, index) =>
                        {
                            Assert.Equal(e, actual[index]);
                            return true;
                        });
                });
        }

        #endregion

        [Fact]
        public void Serializer_SerializeHashSet()
        {
            var expected = new HashSet<ClassOfInt>
            {
                ClassOfInt.Create(true),
                ClassOfInt.Create(true)
            };

            RoundTripSerializationWithCheck(
                expected,
                actual => Assert.True(expected.SequenceEqual(actual)));
        }

        #region Dictionary Tests

        [Fact]
        public void Serializer_SerializeInheritedDictionary()
        {
            RoundTripSerializationWithCheck(DictionaryInheritedClass.Create());
        }

        [Fact]
        public void Serializer_SerializeInheritedUriDictionaryWithNestedType()
        {
            var testObject = new DictionaryInheritedClass<Uri, NestedClass>
            {
                { new Uri("https://1"), NestedClass.Create(true) },
                { new Uri("https://2"), NestedClass.Create(true) },
                { new Uri("https://3"), NestedClass.Create(true) }
            };
            RoundTripSerializationWithCheck(testObject);
        }

        [Fact]
        public void Serializer_SerializeStringDictionaryWithStringType()
        {
            var testObject = ContainingDictionaryClass<string, string>.Create(
                new Dictionary<string, string>
                {
                    { "test1", "test2" },
                    { "entry1", "value1" },
                    { "list", "array" }
                });
            RoundTripSerializationWithCheck(testObject);
        }

        [Fact]
        public void Serializer_SerializeStringDictionaryWithRecursiveType()
        {
            var testObject = ContainingDictionaryClass<string, Recursive>.Create(
                new Dictionary<string, Recursive>
                {
                    { "1", Recursive.Create() },
                    { "2", Recursive.Create() },
                    { "3", Recursive.Create() }
                });
            RoundTripSerializationWithCheck(testObject);
        }

        [Fact]
        public void Serializer_SerializeUriDictionaryWithRecursiveType()
        {
            var testObject = ContainingDictionaryClass<Uri, Recursive>.Create(
                new Dictionary<Uri, Recursive>
                {
                    { new Uri("https://1"), Recursive.Create() },
                    { new Uri("https://2"), Recursive.Create() },
                    { new Uri("https://3"), Recursive.Create() }
                });
            RoundTripSerializationWithCheck(testObject);
        }

        [Fact]
        public void Serializer_SerializeStringDictionaryOfNestedType()
        {
            var expected = new Dictionary<string, NestedClass>
                           {
                               { "field1", NestedClass.Create(true) },
                               { "field2", NestedClass.Create(true) }
                           };
            var settings = new AvroSerializerSettings { KnownTypes = new[] { typeof(Dictionary<string, NestedClass>) }, Resolver = new AvroPublicMemberContractResolver(true) };

            RoundTripSerializationWithCheck(
                (IDictionary<string, NestedClass>)expected,
                actual => Assert.True(Utilities.DictionaryEquals(expected, actual)),
                settings);
        }

        [Fact]
        public void Serializer_SerializeClassOfIntDictionaryWithRecursiveType()
        {
            var expected = new Dictionary<ClassOfInt, Recursive>
            {
                { ClassOfInt.Create(false), Recursive.Create() },
                { ClassOfInt.Create(false), Recursive.Create() }
            };

            RoundTripSerializationWithCheck(
                expected,
                actual => Assert.True(Utilities.DictionaryEquals(expected, actual)));
        }

        [Fact]
        public void Serializer_SerializeStringIDictionaryWithStringTypeUsingKnownTypes()
        {
            var expected = Utilities.GetRandom<Dictionary<string, string>>(false);
            var settings = new AvroSerializerSettings { KnownTypes = new[] { typeof(Dictionary<string, string>) } };

            RoundTripSerializationWithCheck(
                (IDictionary<string, string>)expected,
                actual => Assert.True(Utilities.DictionaryEquals(expected, actual)),
                settings);
        }

        #endregion //Dictionary Tests

        #region DateTime tests

        [Fact]
        public void Serializer_SerializeClassWithDateTimeOffset()
        {
            RoundTripSerializationWithCheck(DateTimeOffsetContainingClass.Create());
        }

        [Fact]
        public void Serializer_SerializeClassWithDateTimeOffsetUsingPosixDateTime()
        {
            var expected = new DateTimeOffsetContainingClass
            {
                LocalTime = DateTimeOffset.Now,
                UtcTime = DateTimeOffset.UtcNow
            };
            var serializer = AvroSerializer.Create<DateTimeOffsetContainingClass>(
                new AvroSerializerSettings { UsePosixTime = true });

            using (var stream = new MemoryStream())
            {
                serializer.Serialize(stream, expected);
                stream.Seek(0, SeekOrigin.Begin);
                var actual = serializer.Deserialize(stream);

                Assert.Equal(expected.LocalTime.Date, actual.LocalTime.Date);
                Assert.Equal(expected.LocalTime.Hour, actual.LocalTime.Hour);
                Assert.Equal(expected.LocalTime.Minute, actual.LocalTime.Minute);
                Assert.Equal(expected.LocalTime.Second, actual.LocalTime.Second);

                Assert.Equal(expected.UtcTime.Date, actual.UtcTime.Date);
                Assert.Equal(expected.UtcTime.Hour, actual.UtcTime.Hour);
                Assert.Equal(expected.UtcTime.Minute, actual.UtcTime.Minute);
                Assert.Equal(expected.UtcTime.Second, actual.UtcTime.Second);
            }
        }

        [Fact]
        public void Serializer_DateTimePosixRoundTrip()
        {
            var expected = new DateTime(1111907664);
            var posixTime = DateTimeSerializer.ConvertDateTimeToPosixTime(expected);
            var actual = DateTimeSerializer.ConvertPosixTimeToDateTime(posixTime);
            Assert.Equal(expected.Hour, actual.Hour);
            Assert.Equal(expected.Minute, actual.Minute);
            Assert.Equal(expected.Second, actual.Second);
        }

        #endregion //DateTime tests

        [Fact]
        public void Serializer_SerializeAnonymousClassDeserializeToAnother()
        {
            var expected = new { IntField = Utilities.GetRandom<int>(false), DoubleField = Utilities.GetRandom<double>(false) };
            var serializer = this.CreateSerializerFor(expected);

            var deserialized = new { AnotherIntField = 0, AnotherDoubleField = 0.0 };
            var deserializer = this.CreateSerializerFor(deserialized);

            using (var stream = new MemoryStream())
            {
                serializer.Serialize(stream, expected);
                stream.Seek(0, SeekOrigin.Begin);
                var actual = Typed(deserialized, deserializer.Deserialize(stream));

                Assert.Equal(expected.DoubleField, actual.AnotherDoubleField);
                Assert.Equal(expected.IntField, actual.AnotherIntField);
            }
        }

        [Fact]
        public void Serializer_SerializeUsingTwoSerializers()
        {
            var expected = ClassWithFields.Create();
            var firstSerializer = AvroSerializer.Create<ClassWithFields>();
            var secondSerializer = AvroSerializer.Create<ClassWithFields>();
            using (var stream = new MemoryStream())
            {
                firstSerializer.Serialize(stream, expected);
                secondSerializer.Serialize(stream, expected);
                stream.Seek(0, SeekOrigin.Begin);

                var actual = firstSerializer.Deserialize(stream);
                Assert.Equal(expected, actual);

                actual = secondSerializer.Deserialize(stream);
                Assert.Equal(expected, actual);
            }
        }

        [Fact]
        public void Serializer_GetNewSerializerInstanceFromCache()
        {
            var cache = new Cache<Tuple<string, Type, AvroSerializerSettings>, GeneratedSerializer>();
            var settings = new AvroSerializerSettings { UsePosixTime = true };
            var key = Tuple.Create(string.Empty, typeof(ClassOfInt), settings);
            var sameKey = Tuple.Create(string.Empty, typeof(ClassOfInt), settings);
            var serializer = new GeneratedSerializer();
            cache.Add(key, serializer);

            var serializer1 = cache.Get(sameKey);
            Assert.Equal(serializer, serializer1);

            settings.UsePosixTime = false;
            var serializer2 = cache.Get(key);

            Assert.Null(serializer2);
        }

        [Fact]
        public void Serializer_CreateIdenticalSerializerIfNoCacheIsUsed()
        {
            Assert.Equal(0, AvroSerializer.CacheEntriesCount);
            var firstSerializer = AvroSerializer.Create<ClassOfInt>(new AvroSerializerSettings { UseCache = true });
            Assert.Equal(1, AvroSerializer.CacheEntriesCount);
            var secondSerializer = AvroSerializer.Create<ClassOfInt>(new AvroSerializerSettings { UseCache = true });
            Assert.Equal(1, AvroSerializer.CacheEntriesCount);
        }

        [Fact]
        public void Serializer_SerializeWithoutGeneratingSerializer()
        {
            try
            {
                var expected = ClassOfInt.Create(true);
                var serializer = AvroSerializer.Create<ClassOfInt>(new AvroSerializerSettings { GenerateSerializer = false });
                using (var stream = new MemoryStream())
                {
                    serializer.Serialize(stream, expected);
                }

                Assert.True(false, "Exception should be thrown.");
            }
            catch (InvalidOperationException ex)
            {
                Assert.Contains("Serialization is not supported. Please change the serialization settings.", ex.Message);
            }
        }

        [Fact]
        public void Serializer_DeserializeWithoutGeneratingDeserializer()
        {
            try
            {
                var serializer = AvroSerializer.Create<ClassOfInt>(new AvroSerializerSettings { GenerateDeserializer = false });
                using (var stream = new MemoryStream())
                {
                    serializer.Deserialize(stream);
                }

                Assert.True(false, "Exception should be thrown.");
            }
            catch (InvalidOperationException ex)
            {
                Assert.Contains("Deserialization is not supported. Please change the serialization settings.", ex.Message);
            }
        }

        #region Concurrency and performance tests

        [Fact]
        public void Serializer_MultithreadSerializationRecursiveType()
        {
            const int NumberOfThreads = 10;
            const int NumberOfSerializationsPerThread = 2000;

            var failed = 0;
            var serializer = AvroSerializer.Create<Recursive>(new AvroSerializerSettings { Resolver = new AvroDataContractResolver(true) });
            var threads = new List<Thread>();
            for (var i = 0; i < NumberOfThreads; ++i)
            {
                threads.Add(new Thread(
                    () =>
                    {
                        try
                        {
                            var expected = Recursive.Create();
                            using (var stream = new MemoryStream())
                            {
                                for (var j = 0; j < NumberOfSerializationsPerThread; j++)
                                {
                                    serializer.Serialize(stream, expected);
                                }
                                stream.Seek(0, SeekOrigin.Begin);

                                for (var j = 0; j < NumberOfSerializationsPerThread; j++)
                                {
                                    var actual = serializer.Deserialize(stream);
                                    Assert.Equal(expected, actual);
                                }
                            }
                        }
                        catch (Exception)
                        {
                            Interlocked.Increment(ref failed);
                            throw;
                        }
                    }));
            }

            threads.ForEach(t => t.Start());
            threads.ForEach(t => t.Join());
            Assert.Equal(0, failed);
        }

        [Fact]
        public void Serializer_MultithreadedSerialization()
        {
            var expected = SimpleFlatClass.Create();
            var threads = new List<Thread>();
            var failedThreads = 0;
            for (var i = 0; i < 32; ++i)
            {
                threads.Add(
                    new Thread(
                        input =>
                        {
                            try
                            {
                                var serializer = input as IAvroSerializer<SimpleFlatClass>;
                                Assert.NotNull(serializer);
                                using (var stream = new MemoryStream())
                                {
                                    var expectedValues = new List<SimpleFlatClass>();
                                    for (var j = 0; j < 1024; ++j)
                                    {
                                        serializer.Serialize(stream, expected);
                                        expectedValues.Add(expected);
                                    }
                                    stream.Seek(0, SeekOrigin.Begin);

                                    var actualValues = new List<SimpleFlatClass>();
                                    for (var j = 0; j < 1024; ++j)
                                    {
                                        actualValues.Add(serializer.Deserialize(stream));
                                    }

                                    Assert.Equal(expectedValues, actualValues);
                                }
                            }
                            catch (Exception)
                            {
                                Interlocked.Increment(ref failedThreads);
                                throw;
                            }
                        }));
            }

            var s = AvroSerializer.Create<SimpleFlatClass>(new AvroSerializerSettings { Resolver = new AvroDataContractResolver(true) });
            threads.ForEach(t => t.Start(s));
            threads.ForEach(t => t.Join());
            Assert.Equal(0, failedThreads);
        }

        [SuppressMessage("Microsoft.Reliability", "CA2000:Dispose objects before losing scope", Justification = "Dispose is called."), Fact]
        public void Serializer_ConcurrentCollections()
        {
            {
                var blocking = new BlockingCollection<Recursive> { Recursive.Create(), Recursive.Create() };
                RoundTripSerializationWithCheck(blocking, actual => Assert.True(blocking.SequenceEqual(actual)));
                blocking.Dispose();
            }

            {
                var bag = new ConcurrentBag<NullableStruct> { NullableStruct.Create(), NullableStruct.Create() };
                RoundTripSerializationWithCheck(bag, actual => Assert.True(bag.SequenceEqual(actual)));
            }

            {
                var queue = new ConcurrentQueue<List<Recursive>>();
                queue.Enqueue(new List<Recursive> { Recursive.Create() });
                queue.Enqueue(new List<Recursive> { Recursive.Create() });
                RoundTripSerializationWithCheck(
                    queue,
                    actual =>
                    {
                        Assert.Equal(queue.Count, actual.Count);
                        queue.Select(
                            (e, index) =>
                            {
                                List<Recursive> elem;
                                actual.TryDequeue(out elem);
                                Assert.Equal(e, elem);
                                return true;
                            });
                    });
            }

            {
                var stack = new ConcurrentStack<NullableStruct>();
                stack.Push(NullableStruct.Create());
                stack.Push(NullableStruct.Create());
                RoundTripSerializationWithCheck(stack, actual => Assert.True(stack.SequenceEqual(actual)));
            }
        }

        [Fact]
        public void Serializer_SerializeHugeObjects()
        {
            var expected = new SimpleFlatClass
            {
                StringField = new string('a', 128000),
                ByteArrayField = Encoding.ASCII.GetBytes(new string('b', 65666)),
                ZeroByteArrayField = Encoding.ASCII.GetBytes(new string('c', 128344))
            };

            RoundTripSerializationWithCheck(
                expected,
                actual => Assert.Equal(expected, actual));
        }

        #endregion //Concurrency and performance tests

        #region Resolvers tests

        [Fact]
        public void Serializer_SerializeNonDataContractClassUsingDataContractResolver()
        {
            try
            {
                var serializer = AvroSerializer.Create<NonDataContractClassWithFields>(
                    new AvroSerializerSettings());
                serializer.Serialize((Stream)null, null);
                Assert.True(false, "Exception should be thrown.");
            }
            catch (SerializationException ex)
            {
                Assert.Contains("Type 'Microsoft.Hadoop.Avro.Tests.NonDataContractClassWithFields' is not supported by the resolver.", ex.Message);
            }
        }

        [Fact]
        public void Serializer_SerializeDataContractClassUsingDataContractResolver()
        {
            var expected = DataContractClassWithFields.Create();
            expected.NotSerializedValue = 13;
            var settings = new AvroSerializerSettings();

            var actual = RoundTripSerializationWithCheck(expected, settings);
            Assert.NotEqual(expected.NotSerializedValue, actual.NotSerializedValue);
        }

        [Fact]
        public void Serializer_SerializeInheritedClassWithDataContractResolver()
        {
            var expected = InheritedSimpleInt.Create();
            var settings = new AvroSerializerSettings();

            var actual = RoundTripSerializationWithCheck(expected, settings);
            Assert.Equal(expected.DataMemberIntProperty, actual.DataMemberIntProperty);
            Assert.NotEqual(expected.PublicIntProperty, actual.PublicIntProperty);
        }

        [Fact]
        public void Serializer_SerializeInheritedClassWithWithPublicMembersResolver()
        {
            var expected = InheritedSimpleInt.Create();

            var actual = RoundTripSerializationWithCheck(expected, new AvroSerializerSettings { Resolver = new AvroPublicMemberContractResolver() });
            Assert.Equal(expected.PublicIntProperty, actual.PublicIntProperty);
            Assert.NotEqual(expected.DataMemberIntProperty, actual.DataMemberIntProperty);
        }

        [Fact]
        public void Serializer_SerializeUnsupportedMembersUsingCustomResolver()
        {
            Assert.Throws<SerializationException>(() =>
                {
                    var serializer =
                        AvroSerializer.Create<ClassOfEvents>(new AvroSerializerSettings
                        {
                            Resolver = new DummyEventResolver()
                        });
                }
            );
        }

        #endregion //Resolvers tests

        #region tests of nulls

        [Fact]
        public void Serializer_SerializeToNullStream()
        {
            Assert.Throws<ArgumentNullException>(() =>
                {
                    var expected = ClassOfInt.Create(true);
                    var serializer = AvroSerializer.Create<ClassOfInt>();
                    serializer.Serialize((Stream) null, expected);
                }
            );
        }

        [Fact]
        public void Serializer_DeserializeNullStream()
        {
            Assert.Throws<ArgumentNullException>(() =>
                {
                    var serializer = AvroSerializer.Create<ClassOfInt>();
                    serializer.Deserialize((Stream) null);
                }
            );
        }

        [Fact]
        public void Serializer_SerializeUsingNullEncoder()
        {
            Assert.Throws<ArgumentNullException>(() =>
                {
                    var serializer = AvroSerializer.Create<int>();
                    serializer.Serialize((IEncoder) null, 0);
                }
            );
        }

        [Fact]
        public void Serializer_DeserializeUsingNullDecoder()
        {
            Assert.Throws<ArgumentNullException>(() =>
                {
                    var serializer = AvroSerializer.Create<int>();
                    serializer.Deserialize((IDecoder) null);
                }
            );
        }

        [Fact]
        [SuppressMessage("Microsoft.Usage", "CA1806:DoNotIgnoreMethodResults",
            Justification = "This test only verifies whether the constructor will throw as expected.")]
        public void Serializer_CreationWithNullSettings()
        {
            Assert.Throws<ArgumentNullException>(() =>
                {
                    AvroSerializer.Create<ClassOfInt>(null);
                }
            );
        }

        [Fact]
        public void Serializer_SerializerNullObject()
        {
            Assert.Throws<SerializationException>(() =>
                {
                    var settings = new AvroSerializerSettings();
                    RoundTripSerializationWithCheck<ClassOfInt>(null, settings);
                }
            );
        }

        [Fact]
        public void Serializer_SerializeNullValues()
        {
            var expected = new[] { "string1", "string2", null };

            RoundTripSerializationWithCheck(
                expected,
                actual => Assert.Equal(expected, actual));
        }

        [Fact]
        public void Serializer_CreateDeserializerOnlyWithNullSchema()
        {
            Assert.Throws<ArgumentNullException>(() =>
                {
                    AvroSerializer.CreateDeserializerOnly<ClassOfInt>(null, new AvroSerializerSettings());
                }
            );
        }

        [Fact]
        public void Serializer_CreateDeserializerOnlyWithNullSettings()
        {
            Assert.Throws<ArgumentNullException>(() =>
                {
                    const string StringSchema = @"{
                             ""type"":""record"",
                             ""name"":""Microsoft.Hadoop.Avro.Tests.ClassOfInt"",
                             ""fields"":
                                       [
                                           {
                                               ""name"":""PrimitiveInt"",
                                               ""type"":""int""
                                           }
                                       ]
                         }";
                    AvroSerializer.CreateDeserializerOnly<ClassOfInt>(StringSchema, null);
                }
            );
        }

        [SuppressMessage("Microsoft.Usage", "CA1806:DoNotIgnoreMethodResults", 
            Justification = "This test only verifies whether the method will throw as expected.")]
        [Fact]
        public void Serializer_ObjectSerializerBaseWithNullArguments()
        {
            Utilities.ShouldThrow<ArgumentNullException>(() => new DummyObjectSerializerBase(null));
            var dummyObjectSerializer = new DummyObjectSerializerBase(new NullSchema());
            Utilities.ShouldThrow<ArgumentNullException>(() => dummyObjectSerializer.BuildSerializer(null, Expression.Empty()));
            Utilities.ShouldThrow<ArgumentNullException>(() => dummyObjectSerializer.BuildSerializer(Expression.Empty(), null));
            Utilities.ShouldThrow<ArgumentNullException>(() => dummyObjectSerializer.BuildDeserializer(null));
            Utilities.ShouldThrow<ArgumentNullException>(() => dummyObjectSerializer.BuildSkipper(null));
            Utilities.ShouldThrow<ArgumentNullException>(() => dummyObjectSerializer.Serialize(null, ClassOfInt.Create(false)));
            Utilities.ShouldThrow<ArgumentNullException>(() => dummyObjectSerializer.Deserialize(null));
            Utilities.ShouldThrow<InvalidOperationException>(() => dummyObjectSerializer.BuildSerializerSafeDelegate(null, null));
            Utilities.ShouldThrow<InvalidOperationException>(() => dummyObjectSerializer.BuildDeserializerSafeDelegate(null));
            Utilities.ShouldThrow<InvalidOperationException>(() => dummyObjectSerializer.BuildSkipperSafeDelegate(null));
            Utilities.ShouldThrow<InvalidOperationException>(() => dummyObjectSerializer.SerializeSafeDelegate(null, null));
            Utilities.ShouldThrow<InvalidOperationException>(() => dummyObjectSerializer.DeserializeSafeDelegate(null));
            Utilities.ShouldThrow<InvalidOperationException>(() => dummyObjectSerializer.SkipSafeDelegate(null));
        }

        #endregion //tests of nulls

        #region Surrogates tests

        [Fact]
        public void Serializer_SerializeClassWithoutParameterlessConstructor()
        {
            var expected = ClassWithoutParameterlessConstructor.Create();
            var settings = new AvroSerializerSettings { Surrogate = new Surrogate(), Resolver = new AvroDataContractResolver(true) };

            RoundTripSerializationWithCheck(expected, settings);
        }

        [Fact]
        public void Serializer_SerializeInheritedStringDictionaryWithSurrogates()
        {
            var expected = new DictionaryInheritedClass<string, ClassWithoutParameterlessConstructor>
            {
                { "test", ClassWithoutParameterlessConstructor.Create() },
                { "test2", ClassWithoutParameterlessConstructor.Create() }
            };
            var settings = new AvroSerializerSettings { Surrogate = new Surrogate(), Resolver = new AvroDataContractResolver(true) };

            RoundTripSerializationWithCheck(expected, settings);
        }

        [Fact]
        public void Serializer_SerializeInheritedListWithSurrogates()
        {
            var expected = new ListInheritedClass<ClassWithoutParameterlessConstructor>
            {
                ClassWithoutParameterlessConstructor.Create(),
                ClassWithoutParameterlessConstructor.Create()
            };
            var settings = new AvroSerializerSettings { Surrogate = new Surrogate(), Resolver = new AvroDataContractResolver(true) };

            RoundTripSerializationWithCheck(expected, settings);
        }

        [Fact]
        public void Serializer_SerializeUriDictionaryWithSurrogate()
        {
            var expected = new Dictionary<Uri, ClassWithoutParameterlessConstructor>
            {
                {
                    new Uri("bing://test1"),
                    ClassWithoutParameterlessConstructor.Create()
                },
                {
                    new Uri("bing://test2"),
                    ClassWithoutParameterlessConstructor.Create()
                }
            };

            var settings = new AvroSerializerSettings { Surrogate = new Surrogate(), Resolver = new AvroDataContractResolver(true) };
            RoundTripSerializationWithCheck(
                expected,
                actual => Utilities.DictionaryEquals(expected, actual),
                settings);
        }

        [Fact]
        public void Serializer_SerializeListWithSurrogate()
        {
            var expected = new List<AnotherClassWithoutParameterlessConstructor>
            {
                AnotherClassWithoutParameterlessConstructor.Create(),
                AnotherClassWithoutParameterlessConstructor.Create(),
                AnotherClassWithoutParameterlessConstructor.Create()
            };

            var settings = new AvroSerializerSettings { Surrogate = new Surrogate(), Resolver = new AvroDataContractResolver(true) };
            RoundTripSerializationWithCheck(
                expected,
                actual => Assert.Equal(expected, actual),
                settings);
        }

        [Fact]
        public void Serializer_SerializeWithSurrogateThatDoesNotSupportTypes()
        {
            Assert.Throws<SerializationException>(() =>
                {
                    var expected = new List<Surrogate>
                    {
                        new Surrogate(),
                        new Surrogate(),
                        new Surrogate()
                    };

                    var settings = new AvroSerializerSettings
                    {
                        Surrogate = new Surrogate(),
                        Resolver = new AvroDataContractResolver(true)
                    };
                    RoundTripSerializationWithCheck(
                        expected,
                        actual => Assert.Equal(expected, actual),
                        settings);
                }
            );
        }

        #endregion //Surrogates tests

        #region KnownTypes tests

        [Fact]
        public void Serializer_SerializeAbstractClassUsingDataContractKnownTypes()
        {
            var settings = new AvroSerializerSettings
                           {
                               Resolver = new AvroDataContractResolver(),
                               KnownTypes = new List<Type> { typeof(Rectangle), typeof(Square) }
                           };
            var expected = new AbstractShape[] { Rectangle.Create(), Square.Create() };
            RoundTripSerializationWithCheck(
                expected,
                actual => Assert.Equal(expected, actual),
                settings);
        }

        [Fact]
        public void Serializer_SerializeAbstractClassUsingKnownTypesInSettings()
        {
            var expected = new AbstractShape[] { Rectangle.Create(), Square.Create() };

            RoundTripSerializationWithCheck(
                expected,
                actual => Assert.Equal(expected, actual),
                new AvroSerializerSettings { KnownTypes = new HashSet<Type> { typeof(Rectangle), typeof(Square) } });
        }

        [Fact]
        public void Serializer_SerializeConcreteBaseClassUsingDataContractKnownTypes()
        {
            var expected = ConcreteShape.Create();

            RoundTripSerializationWithCheck(
                expected,
                actual => Assert.Equal(expected, actual),
                new AvroSerializerSettings { Resolver = new AvroDataContractResolver() });
        }

        [Fact]
        public void Serializer_SerializeConcreteBaseClassUsingKnownTypesInSettings()
        {
            var expected = ConcreteShape.Create();

            RoundTripSerializationWithCheck(
                expected,
                actual => Assert.Equal(expected, actual),
                new AvroSerializerSettings { KnownTypes = new HashSet<Type> { typeof(SquareInheritingConcreteConcreteShape) } });
        }

        [Fact]
        public void Serializer_SerializeClassWithAbstractAndInterfaceMembersUsingKnownTypesInSettings()
        {
            var expected = ClassWithAbstractMembers.Create();

            RoundTripSerializationWithCheck(
                expected,
                new AvroSerializerSettings { KnownTypes = new HashSet<Type> { typeof(Rectangle), typeof(Square), typeof(AnotherSquare), typeof(ClassImplementingInterface) } });
        }

        [Fact]
        public void Serializer_SerializeClassWithAbstractAndInterfaceMembersUsingDataContractKnownTypes()
        {
            var expected = ClassWithAbstractMembers.Create();

            RoundTripSerializationWithCheck(
                expected,
                new AvroSerializerSettings { KnownTypes = new HashSet<Type> { typeof(ClassImplementingInterface) } });
        }

        [Fact]
        public void Serializer_SerializeInterfaceUsingKnownTypesInSettings()
        {
            IInterface expected = ClassImplementingInterface.Create();

            RoundTripSerializationWithCheck(
                expected,
                new AvroSerializerSettings { KnownTypes = new HashSet<Type> { typeof(ClassImplementingInterface) } });
        }

        [Fact]
        public void Serializer_SerializeInheritedClassesUsingKnownTypesInSettings()
        {
            IInterface expected = InheritedClassImplementingInterface.CreateInherited();

            RoundTripSerializationWithCheck(
                expected,
                new AvroSerializerSettings { KnownTypes = new HashSet<Type> { typeof(ClassImplementingInterface), typeof(InheritedClassImplementingInterface) } });
        }

        [Fact]
        public void Serializer_SerializeAbstractClassWithInvalidDataContractKnownTypes()
        {
            Assert.Throws<SerializationException>(() =>
                {
                    AvroSerializer.Create<AbstractClassWithInvalidKnownTypes>();
                }
            );
        }

        [Fact]
        public void Serializer_SerializeAbstractClassWithInvalidKnownTypesInSettings()
        {
            Assert.Throws<SerializationException>(() =>
                {
                    AvroSerializer.Create<AbstractClassWithInvalidKnownTypes>(new AvroSerializerSettings
                    {
                        KnownTypes = new HashSet<Type> {typeof(Rectangle), typeof(Square)}
                    });
                }
            );
        }

        [Fact]
        public void Serializer_SerializeInterfaceWithInvalidKnownTypesInSettings()
        {
            Assert.Throws<SerializationException>(() =>
                {
                    IInterface expected = ClassImplementingInterface.Create();

                    RoundTripSerializationWithCheck(
                        expected,
                        new AvroSerializerSettings {KnownTypes = new HashSet<Type> {typeof(Rectangle), typeof(Square)}});
                }
            );
        }

        [Fact]
        public void Serializer_SerializeInheritedClass()
        {
            Square expected = DifferentSquare.Create();
            var settings = new AvroSerializerSettings { KnownTypes = new List<Type> { typeof(DifferentSquare) } };

            RoundTripSerializationWithCheck(
                expected,
                settings);
        }

        [Fact]
        public void Serializer_SerializeBaseClass()
        {
            Square expected = Square.Create();
            var settings = new AvroSerializerSettings { KnownTypes = new List<Type> { typeof(DifferentSquare) } };

            RoundTripSerializationWithCheck(
                expected,
                settings);
        }

        [Fact]
        public void Serializer_GiveInterfaceInKnownTypes()
        {
            Assert.Throws<SerializationException>(() =>
                {
                    var settings = new AvroSerializerSettings
                    {
                        KnownTypes = new List<Type> {typeof(IInheritedInterface)}
                    };
                    AvroSerializer.Create<IInterface>(settings);
                }
            );
        }

        [Fact]
        public void Serializer_ProvidingSameKnownTypeAsSerialized()
        {
            DifferentSquare expected = DifferentSquare.Create();
            var settings = new AvroSerializerSettings { KnownTypes = new List<Type> { typeof(DifferentSquare) } };

            RoundTripSerializationWithCheck(
                expected,
                settings);
        }

        [Fact]
        public void Serializer_SerializeInheritedClassWithBaseHavingKnownTypes()
        {
            var expected = ClassInheritingClassWithAbstractMembers.Create();
            RoundTripSerializationWithCheck(expected);
        }

        #endregion //KnownTypes tests

        #region Nullables tests

        [Fact]
        public void Serializer_SerializeClassWithNullableFields()
        {
            var expected = NullableFieldsClass.Create();
            RoundTripSerializationWithCheck(expected, new AvroSerializerSettings { Resolver = new AvroDataContractResolver(true) });
        }

        [Fact]
        public void Serializer_SerializeNullableInt()
        {
            int? expected = 10;
            RoundTripSerializationWithCheck(expected);
        }

        [Fact]
        public void Serializer_SerializeNullableIntWithNullValue()
        {
            int? expected = null;
            RoundTripSerializationWithCheck(expected);
        }

        #endregion //Nullables tests

        #region NullableSchema tests

        [Fact]
        public void Serializer_SerializerClassWithNullableSchemaFields()
        {
            var settings = new AvroSerializerSettings();
            var expected = ClassWithFields.Create();
            RoundTripSerializationWithCheck(expected, settings);
        }

        [Fact]
        public void Serializer_SerializerClassWithSchemaNullableFieldUsingDataContractResolverWithCSharpNulls()
        {
            var settings = new AvroSerializerSettings { Resolver = new AvroDataContractResolver(true) };
            var expected = ClassWithSchemaNullableField.Create();
            RoundTripSerializationWithCheck(expected, settings);
        }

        [Fact]
        public void Serializer_SerializerClassWithSchemaNullableFieldUsingDataContractResolverWithNoNulls()
        {
            var settings = new AvroSerializerSettings();
            var expected = ClassWithSchemaNullableField.Create();
            RoundTripSerializationWithCheck(expected, settings);
        }

        [Fact]
        public void Serializer_SerializerClassWithSchemaNullableFieldUsingPublicMembersResolverWithCSharpNulls()
        {
            var settings = new AvroSerializerSettings { Resolver = new AvroDataContractResolver(true) };
            var expected = ClassWithSchemaNullableField.Create();
            RoundTripSerializationWithCheck(expected, settings);
        }

        [Fact]
        public void Serializer_SerializerClassWithSchemaNullableFieldUsingPublicMembersResolverWithNoNulls()
        {
            var settings = new AvroSerializerSettings();
            var expected = ClassWithSchemaNullableField.Create();
            RoundTripSerializationWithCheck(expected, settings);
        }

        #endregion //NullableSchema tests

        [Fact]
        public void SerializerSettings_EqualityTest()
        {
            var settings = new AvroSerializerSettings { GenerateSerializer = false, KnownTypes = new List<Type> { typeof(Square) } };
            var sameSettings = new AvroSerializerSettings { GenerateSerializer = false, KnownTypes = new List<Type> { typeof(Square) } };
            var differentSettings = new AvroSerializerSettings { GenerateSerializer = true, };
            Utilities.VerifyEquality(settings, sameSettings, differentSettings);
        }

        #region Helper methods
        private IAvroSerializer<T> CreateSerializerFor<T>(T obj)
        {
            return AvroSerializer.Create<T>(new AvroSerializerSettings { Resolver = new AvroPublicMemberContractResolver() });
        }

        private static T Typed<T>(T obj, object value)
        {
            return (T)value;
        }

        private static TS RoundTripSerializationWithCheck<TS>(TS serialized, AvroSerializerSettings settings = null)
        {
            return RoundTripSerializationWithCheck(
                serialized,
                actual => Assert.Equal(serialized, actual),
                settings);
        }

        private static TS RoundTripSerializationWithCheck<TS>(TS serialized, Action<TS> check, AvroSerializerSettings settings = null)
        {
            if (check == null)
            {
                throw new ArgumentNullException("check");
            }

            var serializer = settings == null
                ? AvroSerializer.Create<TS>(new AvroSerializerSettings { Resolver = new AvroDataContractResolver(true) })
                : AvroSerializer.Create<TS>(settings);

            using (var stream = new MemoryStream())
            {
                serializer.Serialize(stream, serialized);
                stream.Seek(0, SeekOrigin.Begin);
                var deserialized = serializer.Deserialize(stream);

                check(deserialized);
                return deserialized;
            }
        }
        #endregion

        #region AvroUnion tests

        [Fact]
        public void Serializer_SerializeUnionOfIntStringNull()
        {
            var expected = ClassOfUnion.Create();
            var serializer = AvroSerializer.Create<ClassOfUnion>();

            var fieldType =
                ((RecordSchema)serializer.WriterSchema).Fields.First(f => f.Name == "IntClassOfIntNullFieldClassOfInt").TypeSchema as UnionSchema;
            Assert.NotNull(fieldType);
            Assert.True(fieldType.Schemas[0].RuntimeType == typeof(int));
            Assert.True(fieldType.Schemas[1].RuntimeType == typeof(ClassOfInt));
            Assert.True(fieldType.Schemas[2].RuntimeType == typeof(AvroNull));
            using (var stream = new MemoryStream())
            {
                serializer.Serialize(stream, expected);
                stream.Seek(0, SeekOrigin.Begin);
                var actual = serializer.Deserialize(stream);

                Assert.Equal(expected, actual);
            }
        }

        [Fact]
        public void Serializer_SeiralizeUnionOnIntStringNullUsingWrongValue()
        {
            Assert.Throws<SerializationException>(() =>
                {
                    var obj = ClassOfUnion.Create();
                    obj.IntStringNullFieldInt = Utilities.GetRandom<float>(false);
                    RoundTripSerializationWithCheck(obj);
                }
            );
        }

        [Fact]
        public void Serializer_AvroUnionBackwardCompatbilityWithKnownTypes()
        {
            var expected = ClassWithKnownTypesAndAvroUnion.Create();
            RoundTripSerializationWithCheck(expected);
        }

        [Fact]
        public void Serializer_SerializeClassOfUnion()
        {
            // support multiple collections type as known types.
            var serializer = AvroSerializer.Create<ClassOfUnion>();
            var deserializer = AvroSerializer.CreateDeserializerOnly<ClassOfUnion>(serializer.WriterSchema.ToString(), new AvroSerializerSettings());

            // check for ClassOfUnion
            var expected = ClassOfUnion.Create();
            RoundTripSerializationWithCheck(serializer, deserializer, expected);
        }

        [Fact]
        public void Serializer_SerializeClassWith2ArrayMapKnownTypes()
        {
            var serializer = AvroSerializer.Create<ClassOfUnionWith2ArrayAndMap>();
            var deserializer = AvroSerializer.CreateDeserializerOnly<ClassOfUnionWith2ArrayAndMap>(serializer.WriterSchema.ToString(), new AvroSerializerSettings());

            // check for ClassOfUnionWith2ArrayAndMap
            var expected = ClassOfUnionWith2ArrayAndMap.Create();
            RoundTripSerializationWithCheck(serializer, deserializer, expected);
        }

        [Fact]
        public void Serializer_SerializeClassOfUnionWith2SameArrayAndMap()
        {
            //  according to avro spec http://avro.apache.org/docs/current/spec.html#Unions
            //  Unions may not contain more than one schema with the same type, except for the named types record, fixed and enum. 
            //  For example, unions containing two array types or two map types are not permitted, but two types with different names are permitted                                    
            var serializer = AvroSerializer.Create<ClassOfUnionWith2SameArrayAndMap>();
            string schema = serializer.WriterSchema.ToString();
            
            // assert the actual schema only contains 1 int array and 1 int map, to be compliance with avro spec.
            const string expectedSchema = 
                @"{""type"":""record"","+
                @"""name"":""Microsoft.Hadoop.Avro.Tests.Classes.ClassOfUnionWith2SameArrayAndMap"","+
                @"""fields"":[{""name"":""IntArray"",""type"":[{""type"":""array"",""items"":""int""},""null""]},"+
                @"{""name"":""IntMap"",""type"":[{""type"":""map"",""values"":""int""},""null""]}]}";
            Assert.Equal(expectedSchema, schema);

            var deserializer = AvroSerializer.CreateDeserializerOnly<ClassOfUnionWith2SameArrayAndMap>(schema, new AvroSerializerSettings());

            // check to ensure this class could be deserialize correctly.
            var expected = ClassOfUnionWith2SameArrayAndMap.Create();
            RoundTripSerializationWithCheck(serializer, deserializer, expected);
        }

        [Fact]
        public void Serializer_SerializeClassWithGenericMemberHavingMultipleMatchingKnownTypes()
        {
            // support multiple collections type as known types.
            var knownTypes = new[] {typeof (List<string>), typeof (string[]), typeof (Collection<string>)};
            var serializer = AvroSerializer.Create<IEnumerableClass<string>>(new AvroSerializerSettings() {KnownTypes = knownTypes });
            var deserializer = AvroSerializer.CreateDeserializerOnly<IEnumerableClass<string>>(serializer.WriterSchema.ToString(), new AvroSerializerSettings() { KnownTypes = knownTypes });

            // check for array
            var expected = IEnumerableClass<string>.Create(new [] { "aaa", "bbb", "ccc" });
            RoundTripSerializationWithCheck(serializer, deserializer, expected);

            // check for list
            expected = IEnumerableClass<string>.Create(new List<string> { "aaa", "bbb", "ccc" });
            RoundTripSerializationWithCheck(serializer, deserializer, expected);

            // check for collection
            expected = IEnumerableClass<string>.Create(new Collection<string> { "aaa", "bbb", "ccc" });
            RoundTripSerializationWithCheck(serializer, deserializer, expected);
        }

        [Fact]
        public void UnionSchema_TestIsSameTypeAs()
        {
            TypeSchema intArray1 = new ArraySchema(new IntSchema(typeof(int)), typeof(int[]));
            TypeSchema intArray2 = new ArraySchema(new IntSchema(typeof(int)), typeof(int[]));
            Assert.True(UnionSchema.IsSameTypeAs(intArray1, intArray2));

            TypeSchema stringArray = new ArraySchema(new StringSchema(typeof(string)), typeof(string[]));
            Assert.False(UnionSchema.IsSameTypeAs(intArray1, stringArray));

            TypeSchema intMap1 = new MapSchema(new IntSchema(typeof(int)), new IntSchema(typeof(int)), typeof(Dictionary<int, int>));
            TypeSchema intMap2 = new MapSchema(new IntSchema(typeof(int)), new IntSchema(typeof(int)), typeof(Dictionary<int, int>));
            Assert.True(UnionSchema.IsSameTypeAs(intMap1, intMap2));

            TypeSchema stringMap = new MapSchema(new StringSchema(typeof(string)), new StringSchema(typeof(string)), typeof(Dictionary<string, string>));
            Assert.False(UnionSchema.IsSameTypeAs(intMap1, stringMap));

            Assert.False(UnionSchema.IsSameTypeAs(stringArray, stringMap));
        }

        [Fact]
        public void Serializer_ClassOfObjectDictionary()
        {
            var resolver = new AvroCustomContractResolver();
            var settings = new AvroSerializerSettings() {Resolver = resolver};
            var serializer = AvroSerializer.Create<ClassOfObjectDictionary>(settings);
            var deserializer = AvroSerializer.CreateDeserializerOnly<ClassOfObjectDictionary>(serializer.WriterSchema.ToString(), settings);

            var expected = ClassOfObjectDictionary.Create();
            RoundTripSerializationWithCheck(serializer, deserializer, expected);
        }

        private void RoundTripSerializationWithCheck<TS>(IAvroSerializer<TS> serializer, IAvroSerializer<TS> deserializer, TS serialized)
        {
            using (var stream = new MemoryStream())
            {
                serializer.Serialize(stream, serialized);
                stream.Seek(0, SeekOrigin.Begin);
                var actual = deserializer.Deserialize(stream);

                Assert.Equal(serialized, actual);
            }
        }

        #endregion //AvroUnion tests
    }
}