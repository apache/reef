namespace Org.Apache.REEF.IMRU.API
{
    /// <summary>
    /// Represents the output of an IUpdateFunction.
    /// </summary>
    /// <seealso cref="IUpdateFunction{TMapInput,TMapOutput,TResult}" />
    /// <typeparam name="TMapInput"></typeparam>
    /// <typeparam name="TResult"></typeparam>
    public sealed class UpdateResult<TMapInput, TResult>
    {
        private readonly bool _done;
        private readonly bool _hasMapInput;
        private readonly bool _hasResult;
        private readonly TMapInput _mapInput;
        private readonly TResult _result;

        private UpdateResult(bool done, bool hasMapInput, bool hasResult, TResult result, TMapInput mapInput)
        {
            _result = result;
            _mapInput = mapInput;
            _hasMapInput = hasMapInput;
            _hasResult = hasResult;
            _done = done;
        }

        internal bool IsDone
        {
            get { return _done; }
        }

        internal bool IsNotDone
        {
            get { return !_done; }
        }

        internal bool HasMapInput
        {
            get { return _hasMapInput; }
        }

        internal bool HasResult
        {
            get { return _hasResult; }
        }

        internal TMapInput MapInput
        {
            get { return _mapInput; }
        }

        internal TResult Result
        {
            get { return _result; }
        }

        /// <summary>
        /// Indicate that the IMRU job is done.
        /// </summary>
        /// <param name="result">The result of the job.</param>
        /// <returns></returns>
        public static UpdateResult<TMapInput, TResult> Done(TResult result)
        {
            return new UpdateResult<TMapInput, TResult>(true, false, true, result, default(TMapInput));
        }

        /// <summary>
        /// Indicate that another round of computation is needed.
        /// </summary>
        /// <param name="mapInput">The input to the IMapFunction.Map() method.</param>
        /// <returns></returns>
        public static UpdateResult<TMapInput, TResult> AnotherRound(TMapInput mapInput)
        {
            return new UpdateResult<TMapInput, TResult>(false, true, false, default(TResult), mapInput);
        }

        /// <summary>
        /// Indicate another round and produce some intermediate results.
        /// </summary>
        /// <param name="mapInput">The input to the IMapFunction.Map() method.</param>
        /// <param name="intermediateResult">The intermediate results.</param>
        /// <returns></returns>
        public static UpdateResult<TMapInput, TResult> AnotherRound(TMapInput mapInput, TResult intermediateResult)
        {
            return new UpdateResult<TMapInput, TResult>(false, true, true, intermediateResult, mapInput);
        }
    }
}