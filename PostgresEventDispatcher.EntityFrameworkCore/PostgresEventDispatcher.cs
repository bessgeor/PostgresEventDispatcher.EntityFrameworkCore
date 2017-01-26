using System;
using Npgsql.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore;
using System.Linq;
using System.Text;
using System.Collections.Generic;

namespace Microsoft.Extensions.DependencyInjection
{
	[Flags]
	public enum TriggerType
	{
		Before,
		After,
		InsteadOf
	}

	public interface IPostgresEventDispatcherServiceInitializer { }

	public static class PostgresEventDispatcher
	{
		private static DbContext _db = null;
		private static bool _eventsBegan => _db == null;

		private const string _insertAlertFuncDefinition =
			"CREATE OR REPLACE FUNCTION pg_event_dispatcher_on_insert RETURNS trigger AS $pg_event_dispatcher_on_insert$"
				+ "BEGIN"
					+ "PERFORM pg_notify(TG_ARGV[0], row_to_json(NEW, false));"
				+ "END;"
			+ "$pg_event_dispatcher_on_insert$ LANGUAGE plpgsql;";
		private static bool _insertAlertFuncDefined = false;

		private static StringBuilder _sqlBuilder;
		private static IServiceProvider _provider;

		private class Marker : IPostgresEventDispatcherServiceInitializer { }

		public static IPostgresEventDispatcherServiceInitializer InitPostgresEventsSetupTransactionWithContextType<ContextType>( this IServiceProvider provider )
			where ContextType : DbContext
		{
			if( _eventsBegan )
				throw new InvalidOperationException( "You are trying to init new postgres event setup while not finished the previous one" );
			_db = provider.GetRequiredService<ContextType>();
			_sqlBuilder = new StringBuilder();
			_provider = provider;
			return new Marker();
		}

		public static IServiceProvider CancelPostgresEventsSetupTransaction( this IPostgresEventDispatcherServiceInitializer marker )
		{
			_sqlBuilder = null;
			_db = null;
			IServiceProvider toReturn = _provider;
			_provider = null;
			return toReturn;
		}

		public static IServiceProvider CommitPostgresEventsSetupTransaction( this IPostgresEventDispatcherServiceInitializer marker )
		{
			_db.Database.ExecuteSqlCommand( _sqlBuilder.ToString() );
			return marker.CancelPostgresEventsSetupTransaction();
		}

		public static IPostgresEventDispatcherServiceInitializer AddPostgresRowEventOnInsert<EntityType>( this IPostgresEventDispatcherServiceInitializer marker, TriggerType type )
			where EntityType : class
		{
			if( type != TriggerType.After )
				throw new NotImplementedException( "There is no support for BEFORE and INSTEAD OF triggers yet." );

			if( !_insertAlertFuncDefined )
			{
				_sqlBuilder = _sqlBuilder.Append( _insertAlertFuncDefinition );
				_insertAlertFuncDefined = true;
			}

			string entityDbName = _db.Model.FindEntityType( typeof( EntityType ) ).Model.Npgsql().DatabaseName;

			_sqlBuilder = _sqlBuilder.Append( "CREATE TRIGGER IF NOT EXISTS " ).Append( entityDbName ).Append( "Insertion" ).Append( "AFTER INSERT ON " ).Append( entityDbName )
				.Append( "FOR EACH ROW EXECUTE PROCEDURE pg_event_dispatcher_on_insert('" ).Append( entityDbName ).Append( type ).Append( "Insert');" );

			return marker;
		}
	}
}
