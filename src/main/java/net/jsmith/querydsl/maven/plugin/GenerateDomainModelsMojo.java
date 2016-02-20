package net.jsmith.querydsl.maven.plugin;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

import org.apache.maven.plugin.AbstractMojo;
import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.plugin.MojoFailureException;
import org.apache.maven.plugins.annotations.LifecyclePhase;
import org.apache.maven.plugins.annotations.Mojo;
import org.apache.maven.plugins.annotations.Parameter;
import org.apache.maven.project.MavenProject;

import com.mysema.query.sql.codegen.MetaDataExporter;

@Mojo(
		name = "generate-domain-models",
		defaultPhase = LifecyclePhase.GENERATE_SOURCES
)
public class GenerateDomainModelsMojo extends AbstractMojo {

	@Parameter(
			defaultValue = "${project}"
	)
	private MavenProject mavenProject;
	
	@Parameter(
			defaultValue = "${project.basedir}/domain-desc.sql"
	)
	private File domainDesc;
	
	@Parameter(
			defaultValue = "${project.basedir}/target/generated-sources/java"
	)
	private File modelDirectory;
	
	@Parameter(
			required = true
	)
	private String modelPackage;
	
	public void execute( ) throws MojoExecutionException, MojoFailureException {
		if( !this.domainDesc.exists( ) || !this.domainDesc.isFile( ) ) {
			throw new MojoExecutionException( String.format( "'%s' is not a valid domain description file.", this.domainDesc ) );
		}
		
		if( !this.modelDirectory.exists( ) ) {
			this.modelDirectory.mkdirs( );
		}
		if( !this.modelDirectory.isDirectory( ) ) {
			throw new MojoExecutionException( String.format( "'%s' is not a valid destination directory.", this.modelDirectory ) );
		}
		
		Connection con = null;
		try {
			con = this.createTemporaryH2Database( );
			this.loadDomainModel( con );
			this.writeModelSources( con );
			
			this.mavenProject.addCompileSourceRoot( this.modelDirectory.getAbsolutePath( ) );
		}
		catch( SQLException sqle ) {
			throw new MojoExecutionException( "Failed to load domain model into temporary H2 memory database.", sqle );
		}
		finally {
			if( con != null ) {
				try {
					con.close( );
				}
				catch( SQLException sqle ) { }
			}
		}
	}

	private Connection createTemporaryH2Database( ) throws MojoExecutionException, SQLException {
		try {
			Class.forName( "org.h2.Driver" );
			return DriverManager.getConnection( "jdbc:h2:mem:tmp-domain-db;DB_CLOSE_DELAY=-1" );
		}
		catch( ClassNotFoundException cnfe ) {
			throw new MojoExecutionException( "Failed to load H2 driver class." );
		}
	}
	
	private void loadDomainModel( Connection con ) throws MojoExecutionException, SQLException {
		String domainSQL = "";
		
		BufferedReader domainReader = null;
		try {
			domainReader = new BufferedReader( new FileReader( this.domainDesc ) );
			
			String line = "";
			while( ( line = domainReader.readLine( ) ) != null ) {
				domainSQL = domainSQL + line + "\n";
			}
		}
		catch( IOException ioe ) {
			throw new MojoExecutionException( "Failed to read domain model.", ioe );
		}
		finally {
			try {
				domainReader.close( );
			}
			catch( IOException ioe ) { }
		}
		
		Statement s = null;
		try {
			s = con.createStatement( );
			s.execute( domainSQL );
		}
		finally {
			try {
				s.close( );
			}
			catch( SQLException sqle ) { }
		}
	}
	
	private void writeModelSources( Connection con ) throws MojoExecutionException, SQLException {
		MetaDataExporter exporter = new MetaDataExporter( );
		exporter.setPackageName( this.modelPackage );
		exporter.setTargetFolder( this.modelDirectory );
		
		exporter.export( con.getMetaData( ) );
	}
	
}
